﻿using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.IO;

class RoutingRule
{
    public bool Preprocess { get; set; }
    public string ServerIp { get; set; }
    public int ServerPort { get; set; }
}

class Aggregator
{
    static Dictionary<string, string> wavyStates;
    static Dictionary<string, RoutingRule> routingRules;
    static List<string> dataBuffer = new List<string>();  // Armazena os dados recebidos
    static System.Timers.Timer dataTimer;  // Timer para enviar os dados a cada 30 segundos
    static System.Timers.Timer collectTimer;  // Timer para coletar dados por 10 segundos
    static bool isRunning = true; // Flag para controlar a execução do servidor

    static void Main()
    {
        wavyStates = LoadWavyStates("waves.csv");
        routingRules = LoadRoutingRules("routing.csv");

        // Inicializa o listener para aceitar conexões dos dispositivos WAVY
        TcpListener listener = new TcpListener(IPAddress.Any, 5000);
        listener.Start();
        Console.WriteLine("Aggregator started, waiting for WAVY devices...");

        // Timer para enviar os dados acumulados a cada 30 segundos
        dataTimer = new System.Timers.Timer(30000); // 30 segundos
        dataTimer.Elapsed += (sender, e) => SendDataToServer(); // Envia dados ao servidor a cada 30 segundos
        dataTimer.Start();

        // Timer para coletar os dados a cada 10 segundos
        collectTimer = new System.Timers.Timer(10000); // 10 segundos
        collectTimer.Elapsed += (sender, e) => CollectData(); // Coleta dados a cada 10 segundos
        collectTimer.Start();

        while (isRunning) // A execução do servidor depende da flag isRunning
        {
            TcpClient wavyClient = listener.AcceptTcpClient();
            Thread t = new Thread(() => HandleWavy(wavyClient));
            t.Start();
        }

        Console.WriteLine("Aggregator stopped.");
    }

    static void HandleWavy(TcpClient wavyClient)
    {
        NetworkStream wavyStream = wavyClient.GetStream();
        byte[] buffer = new byte[1024];
        int bytesRead;

        // Lê as mensagens enviadas pelo WAVY
        while ((bytesRead = wavyStream.Read(buffer, 0, buffer.Length)) > 0)
        {
            string message = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
            Console.WriteLine($"WAVY Sent: {message}");

            // Processa comandos de registro
            if (message.StartsWith("REGISTER"))
            {
                string[] parts = message.Split(' ');
                if (parts.Length >= 2)
                {
                    string wavyId = parts[1].Trim();

                    // Verifica se o WAVY está associado
                    if (!wavyStates.ContainsKey(wavyId))
                    {
                        Console.WriteLine($"❌ WAVY {wavyId} não está associada.");
                        SendResponseToWavy(wavyStream, "403 NOT ASSOCIATED");
                        continue;
                    }

                    // Verifica se o WAVY está em estado de operação
                    if (wavyStates[wavyId] != "operacao")
                    {
                        Console.WriteLine($"⚠️ WAVY {wavyId} está em estado '{wavyStates[wavyId]}', operação não permitida.");
                        SendResponseToWavy(wavyStream, $"403 BLOCKED STATE: {wavyStates[wavyId]}");
                        continue;
                    }

                    // Se estiver tudo correto, encaminha o comando para o servidor
                    ForwardToServer($"FORWARD {message}", wavyStream, "127.0.0.1", 5001); // SERVER FIXO PARA REGISTER
                }
            }
            // Processa comandos de dados
            else if (message.StartsWith("DATA"))
            {
                // Adiciona o dado no buffer
                dataBuffer.Add(message);
                Console.WriteLine($"Dado armazenado: {message}");
            }
            // Processa comando de quit
            else if (message.StartsWith("QUIT"))
            {
                Console.WriteLine("🛑 Encerrando a conexão com o WAVY.");
                SendResponseToWavy(wavyStream, "400 BYE");
                isRunning = false;  // Finaliza o servidor
                break;  // Sai do loop e encerra a conexão
            }
        }

        // Fecha a conexão com o cliente WAVY
        wavyClient.Close();
    }

    // Coleta dados por 10 segundos
    static void CollectData()
    {
        // Se não houver dados no buffer, não faz nada
        if (dataBuffer.Count == 0)
            return;

        Console.WriteLine("Coletando dados...");

        // Você pode adicionar lógica aqui para processar os dados se necessário
    }
    // Função para enviar os dados acumulados ao servidor a cada 30 segundos
    static void SendDataToServer()
    {
        if (dataBuffer.Count == 0)
        {
            Console.WriteLine("Nenhum dado para enviar.");
            return;
        }

        // Cria o arquivo CSV para os dados
        string filePath = "collected_data.csv";

        // Cria o cabeçalho do CSV
        var csvLines = new List<string>
    {
        "Timestamp,WavyId,DataType,Value" // Cabeçalho do CSV
    };

        // Adiciona as linhas de dados no CSV com timestamp
        foreach (var data in dataBuffer)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");  // Captura o timestamp
            csvLines.Add($"{timestamp},{data}"); // Adiciona timestamp + dados
        }

        // Grava os dados no arquivo CSV
        try
        {
            File.WriteAllLines(filePath, csvLines);
            Console.WriteLine($"Dados armazenados em {filePath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao escrever o arquivo CSV: {ex.Message}");
            return;
        }

        // Envia os dados para o servidor
        ForwardToServer($"FORWARD FILE {filePath}", null, "127.0.0.1", 5001);

        // Limpa o buffer após enviar os dados
        dataBuffer.Clear();
    }

    // Função para encaminhar a mensagem para o servidor
    static void ForwardToServer(string message, NetworkStream wavyStream, string ip, int port)
    {
        TcpClient serverClient = new TcpClient(ip, port);
        NetworkStream serverStream = serverClient.GetStream();
        byte[] data = Encoding.UTF8.GetBytes(message);
        serverStream.Write(data, 0, data.Length);
        Console.WriteLine($"Aggregator Sent to Server: {message}");

        byte[] buffer = new byte[1024];
        int bytesRead = serverStream.Read(buffer, 0, buffer.Length);
        string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        Console.WriteLine($"Server Response: {response}");

        // Se necessário, envia a resposta do servidor ao WAVY
        if (wavyStream != null)
        {
            SendResponseToWavy(wavyStream, response);
        }

        serverClient.Close();
    }

    // Envia a resposta para o WAVY
    static void SendResponseToWavy(NetworkStream wavyStream, string response)
    {
        byte[] responseData = Encoding.UTF8.GetBytes(response);
        wavyStream.Write(responseData, 0, responseData.Length);
        Console.WriteLine($"Aggregator Sent to WAVY: {response}");
    }

    // Carrega os estados dos dispositivos WAVY a partir de um arquivo CSV
    static Dictionary<string, string> LoadWavyStates(string filePath)
    {
        var wavys = new Dictionary<string, string>();

        if (!File.Exists(filePath))
        {
            Console.WriteLine("⚠️ Ficheiro waves.csv não encontrado.");
            return wavys;
        }

        var lines = File.ReadAllLines(filePath);

        foreach (var line in lines)
        {
            if (line.StartsWith("wavy_id")) continue;

            var parts = line.Split(',');
            if (parts.Length >= 2)
            {
                string id = parts[0].Trim();
                string estado = parts[1].Trim().ToLower();
                wavys[id] = estado;
            }
        }

        return wavys;
    }

    // Carrega as regras de roteamento a partir de um arquivo CSV
    static Dictionary<string, RoutingRule> LoadRoutingRules(string filePath)
    {
        var rules = new Dictionary<string, RoutingRule>();

        if (!File.Exists(filePath))
        {
            Console.WriteLine("⚠️ Ficheiro routing.csv não encontrado.");
            return rules;
        }

        var lines = File.ReadAllLines(filePath);

        foreach (var line in lines)
        {
            if (line.StartsWith("wavy_id")) continue;

            var parts = line.Split(',');
            if (parts.Length >= 5)
            {
                string wavyId = parts[0].Trim();
                string dataType = parts[1].Trim();
                bool preprocess = parts[2].Trim().ToLower() == "true";
                string ip = parts[3].Trim();
                int port = int.Parse(parts[4].Trim());

                string key = $"{wavyId}|{dataType}";
                rules[key] = new RoutingRule
                {
                    Preprocess = preprocess,
                    ServerIp = ip,
                    ServerPort = port
                };
            }
        }

        return rules;
    }

    // Função de pré-processamento dos dados
    static bool Preprocess(string type, string value)
    {
        Console.WriteLine($"[DEBUG] Entrou no Preprocess com type={type}, value={value}");

        if (type == "TEMP" && double.TryParse(value, out double temp))
        {
            if (temp >= 0 && temp <= 40)
            {
                Console.WriteLine("✅ TEMP válido.");
                return true;
            }
            Console.WriteLine($"❌ Valor TEMP inválido: {temp}");
            return false;
        }

        return true; // Por omissão, aceitar outros tipos de dados
    }
}
