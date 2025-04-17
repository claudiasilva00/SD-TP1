using System;
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
    static System.Timers.Timer dataSendTimer;  // Temporizador para enviar os dados a cada 40 segundos
    static bool isRunning = true; // Flag para controlar a execução do servidor
    static TcpListener listener = new TcpListener(IPAddress.Any, 5000);

    static void Main()
    {
        wavyStates = LoadWavyStates("waves.csv");
        routingRules = LoadRoutingRules("routing.csv");

        listener.Start();
        Console.WriteLine("Agregador iniciado, aguardando dispositivos WAVY...");

        // Temporizador para enviar os dados acumulados a cada 40 segundos
        dataSendTimer = new System.Timers.Timer(40000);
        dataSendTimer.Elapsed += (sender, e) => SendDataToServer();
        dataSendTimer.Start();

        // Inicia o thread para lidar com comandos do console
        Thread consoleThread = new Thread(HandleConsoleCommands);
        consoleThread.Start();

        while (isRunning)
        {
            TcpClient wavyClient = listener.AcceptTcpClient();
            Thread t = new Thread(() => HandleWavy(wavyClient));
            t.Start();
        }

        Console.WriteLine("Agregador encerrado.");
    }

    static void HandleConsoleCommands()
    {
        while (isRunning)
        {
            string command = Console.ReadLine()?.Trim();

            if (command.StartsWith("SET_STATE"))
            {
                var parts = command.Split(' ');
                if (parts.Length == 3)
                {
                    SetWavyState(parts[1], parts[2]); // Altera o estado do WAVY
                }
                else
                {
                    Console.WriteLine("Comando inválido. Use SET_STATE {wavy_id} {new_state}");
                }
            }
            else if (command == "FORWARD_QUIT")
            {
                SendForwardQuitToServer();  // Envia FORWARD QUIT para o servidor
            }
            else
            {
                Console.WriteLine("Comando inválido.");
            }
        }
    }

    static void SendForwardQuitToServer()
    {
        ForwardToServer("FORWARD QUIT", null, "127.0.0.1", 5001);

        // Fechar o listener para que ele pare de aceitar novas conexões
        listener.Stop();
        Console.WriteLine("Listener parado.");

        // Fechar o flag isRunning para que o Agregador pare
        isRunning = false;

        // Salva os dados no arquivo CSV antes de encerrar
        SaveCollectedData();

        // Exibe mensagem final
        Console.WriteLine("Agregador encerrado.");
    }

    static void HandleWavy(TcpClient wavyClient)
    {
        NetworkStream wavyStream = null;
        try
        {
            wavyStream = wavyClient.GetStream();
            byte[] buffer = new byte[1024];
            int bytesRead;

            while (isRunning)
            {
                if (wavyStream != null && wavyStream.CanRead) // Verifique se o stream está disponível para leitura
                {
                    bytesRead = wavyStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        string message = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
                        Console.WriteLine($"WAVY Sent: {message}");

                        if (message.StartsWith("REGISTER"))
                        {
                            // Processa o comando REGISTER
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
                                // Envia a resposta para o servidor
                                ForwardToServer($"FORWARD {message}", wavyStream, "127.0.0.1", 5001);
                            }
                        }
                        else if (message.StartsWith("DATA"))
                        {
                            // Armazena o dado no buffer
                            dataBuffer.Add(message);
                            Console.WriteLine($"Dado armazenado: {message}");
                        }
                        else if (message == "QUIT")
                        {
                            // Envia a resposta de encerramento para o WAVY
                            SendResponseToWavy(wavyStream, "400 BYE");

                            // Registra os dados no arquivo CSV antes de encerrar
                            SaveCollectedData();

                            // Fecha a conexão com o WAVY
                            wavyClient.Close();

                            // AQUI NÃO ENCERRAMOS O AGREGADOR. O servidor continua em execução.
                            Console.WriteLine("Conexão com o WAVY encerrada.");
                            break;  // Sai do loop quando o WAVY se desconectar
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao lidar com o cliente WAVY: {ex.Message}");
        }
        finally
        {
            // Certifique-se de fechar o stream e o cliente de forma segura
            if (wavyStream != null)
            {
                wavyStream.Close();
            }

            if (wavyClient != null)
            {
                wavyClient.Close();
            }
        }
    }

    static void SaveCollectedData()
    {
        // Aqui você pode adicionar lógica para garantir que o arquivo CSV seja salvo corretamente
        string filePath = "collected_data.csv";
        var csvLines = new List<string>
    {
        "Timestamp,WavyId,DataType,Value" // Cabeçalho do CSV
    };

        foreach (var data in dataBuffer)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            csvLines.Add($"{timestamp},{data}");
        }

        try
        {
            File.WriteAllLines(filePath, csvLines);
            Console.WriteLine("Dados armazenados em collected_data.csv.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao salvar os dados: {ex.Message}");
        }
    }

    // Função para enviar os dados acumulados ao servidor a cada 30 segundos
    static void SendDataToServer()
    {
        if (dataBuffer.Count == 0)
        {
            Console.WriteLine("Nenhum dado para enviar.");
            return; // Se o buffer estiver vazio, não envia nada
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
    /*// Coleta dados por 10 segundos
        static void CollectData()
        {
            // Se não houver dados no buffer, não faz nada
            if (dataBuffer.Count == 0)
                return;

            Console.WriteLine("Coletando dados...");

            // Você pode adicionar lógica aqui para processar os dados se necessário

        }*/

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
    // Adiciona a função que altera o estado do WAVY no arquivo CSV
    static void SetWavyState(string wavyId, string newState)
    {
        // Carrega os estados dos WAVYs
        var wavys = LoadWavyStates("waves.csv");

        // Verifica se o WAVY existe
        if (wavys.ContainsKey(wavyId))
        {
            // Atualiza o estado do WAVY
            wavys[wavyId] = newState;
            Console.WriteLine($"Estado do WAVY {wavyId} alterado para '{newState}'.");

            // Salva novamente os estados no arquivo
            SaveWavyStates("waves.csv", wavys);
        }
        else
        {
            Console.WriteLine($"WAVY {wavyId} não encontrado.");
        }
    }

    // Função para salvar os estados modificados no arquivo CSV
    static void SaveWavyStates(string filePath, Dictionary<string, string> wavys)
    {
        var csvLines = new List<string> { "wavy_id,estado" };

        foreach (var wavy in wavys)
        {
            csvLines.Add($"{wavy.Key},{wavy.Value}");
        }

        // Escreve no arquivo CSV
        File.WriteAllLines(filePath, csvLines);
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
