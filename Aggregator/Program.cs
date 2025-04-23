using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.IO;
using System.Timers;

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
    static List<string> dataBuffer = new List<string>();
    static System.Timers.Timer dataSendTimer;
    static bool isRunning = true;
    static TcpListener listener = new TcpListener(IPAddress.Any, 5000);

    static void Main()
    {
        // Inicializa os dados do Agregador
        wavyStates = LoadWavyStates("waves.csv");
        routingRules = LoadRoutingRules("routing.csv");

        // Captura Ctrl+C para encerrar corretamente
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;  // Impede o encerramento imediato
            Console.WriteLine("Encerrando agregador com Ctrl+C...");
            // Salva os dados antes de encerrar
            SaveCollectedData();
            SaveWavyStates("waves.csv", wavyStates);
            listener.Stop();  // Para o listener
            isRunning = false; // Interrompe o servidor
            Console.WriteLine("Dados salvos e agregador encerrado.");
        };

        listener.Start();
        Console.WriteLine("Agregador iniciado, aguardando dispositivos WAVY...");

        dataSendTimer = new System.Timers.Timer(40000); // Timer para enviar dados
        dataSendTimer.Elapsed += (sender, e) => SendDataToServer();  // Envia os dados periodicamente
        dataSendTimer.Start();

        Thread consoleThread = new Thread(HandleConsoleCommands);
        consoleThread.Start();

        // Loop principal para aceitar conexões de WAVYs
        while (isRunning)
        {
            try
            {
                TcpClient wavyClient = listener.AcceptTcpClient();
                Thread t = new Thread(() => HandleWavy(wavyClient));
                t.Start();
            }
            catch (SocketException ex)
            {
                Console.WriteLine($"Erro de socket: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro desconhecido: {ex.Message}");
            }
        }

        Console.WriteLine("Agregador encerrado.");
    }

    // Lidar com cada WAVY
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
                if (wavyStream != null && wavyStream.CanRead)
                {
                    bytesRead = wavyStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        string message = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
                        Console.WriteLine($"WAVY Sent: {message}");

                        if (message.StartsWith("REGISTER"))
                        {
                            string[] parts = message.Split(' ');
                            if (parts.Length >= 2)
                            {
                                string wavyId = parts[1].Trim();

                                if (!wavyStates.ContainsKey(wavyId))
                                {
                                    SendResponseToWavy(wavyStream, "403 NOT ASSOCIATED");
                                    continue;
                                }

                                if (wavyStates[wavyId] != "operacao")
                                {
                                    SendResponseToWavy(wavyStream, $"403 BLOCKED STATE: {wavyStates[wavyId]}");
                                    continue;
                                }

                                ForwardToServer($"FORWARD {message}", wavyStream, "127.0.0.1", 5001);
                            }
                        }
                        else if (message.StartsWith("DATA"))
                        {
                            dataBuffer.Add(message);
                            Console.WriteLine($"Dado armazenado: {message}");
                            SendResponseToWavy(wavyStream, "100 OK");
                        }
                        else if (message == "QUIT")
                        {
                            SendResponseToWavy(wavyStream, "400 BYE");
                            SaveCollectedData();  // Garante o salvamento dos dados ao receber QUIT
                            wavyClient.Close();
                            Console.WriteLine("Conexão com o WAVY encerrada.");
                            break;
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
            if (wavyStream != null) wavyStream.Close();
            if (wavyClient != null) wavyClient.Close();
        }
    }
    // Comandos do console
    static void HandleConsoleCommands()
    {
        while (isRunning)
        {
            string command = Console.ReadLine()?.Trim();

            if (command == "STOP DATA")
            {
                // Para o temporizador quando o comando for STOP DATA
                dataSendTimer.Stop();
                Console.WriteLine("Geração de dados parada.");
            }
            else if (command == "FORWARD_QUIT")
            {
                SendForwardQuitToServer();  // Enviar comando para encerrar o servidor de forma controlada
            }
            else if (command.StartsWith("SET_STATE"))
            {
                // Processar o comando SET_STATE para alterar o estado de um WAVY
                string[] parts = command.Split(' ');
                if (parts.Length == 3)
                {
                    string wavyId = parts[1].Trim();
                    string newState = parts[2].Trim();
                    SetWavyState(wavyId, newState);  // Alterar o estado do WAVY
                }
                else
                {
                    Console.WriteLine("Comando SET_STATE inválido. Use: SET_STATE {wavy_id} {estado}");
                }
            }
            else
            {
                Console.WriteLine("Comando inválido.");
            }
        }
    }

    // Enviar FORWARD_QUIT ao servidor
    static void SendForwardQuitToServer()
    {
        try
        {
            // Envia o comando "FORWARD QUIT" para o servidor
            ForwardToServer("FORWARD QUIT", null, "127.0.0.1", 5001);

            // Salvar dados antes de parar o listener
            SaveCollectedData();
            SaveWavyStates("waves.csv", wavyStates);

            listener.Stop();  // Parar o listener de forma controlada
            isRunning = false;  // Definir que o servidor não deve continuar em execução

            Console.WriteLine("Agregador encerrado.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao processar FORWARD QUIT: {ex.Message}");
        }
    }


    // Salvar os dados coletados
    static void SaveCollectedData()
    {
        string filePath = "collected_data.csv";
        var csvLines = new List<string> { "Timestamp,WavyId,DataType,Value" };

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

    // Enviar dados para o servidor
    static void SendDataToServer()
    {
        if (dataBuffer.Count == 0)
        {
            Console.WriteLine("Nenhum dado para enviar.");
            return;
        }

        string filePath = "collected_data.csv";
        var csvLines = new List<string> { "Timestamp,WavyId,DataType,Value" };

        foreach (var data in dataBuffer)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            csvLines.Add($"{timestamp},{data}");
        }

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

        ForwardToServer($"FORWARD FILE {filePath}", null, "127.0.0.1", 5001);
        dataBuffer.Clear();
    }

    // Enviar para o servidor
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

        if (wavyStream != null)
        {
            SendResponseToWavy(wavyStream, response);
        }

        serverClient.Close();
    }

    // Enviar resposta para o WAVY
    static void SendResponseToWavy(NetworkStream wavyStream, string response)
    {
        byte[] responseData = Encoding.UTF8.GetBytes(response);
        wavyStream.Write(responseData, 0, responseData.Length);
        Console.WriteLine($"Aggregator Sent to WAVY: {response}");
    }

    // Atualizar estado do WAVY
    static void SetWavyState(string wavyId, string newState)
    {
        var wavys = LoadWavyStates("waves.csv");

        if (wavys.ContainsKey(wavyId))
        {
            wavys[wavyId] = newState;
            Console.WriteLine($"Estado do WAVY {wavyId} alterado para '{newState}'.");
            SaveWavyStates("waves.csv", wavys); // Salva os estados atualizados
        }
        else
        {
            Console.WriteLine($"WAVY {wavyId} não encontrado.");
        }
    }

    // Salvar estados dos WAVYs
    static void SaveWavyStates(string filePath, Dictionary<string, string> wavys)
    {
        var csvLines = new List<string> { "wavy_id,estado" };
        foreach (var wavy in wavys)
        {
            csvLines.Add($"{wavy.Key},{wavy.Value}");
        }
        File.WriteAllLines(filePath, csvLines);
    }

    // Carregar estados dos WAVYs
    static Dictionary<string, string> LoadWavyStates(string filePath)
    {
        var wavys = new Dictionary<string, string>();
        if (!File.Exists(filePath)) return wavys;

        var lines = File.ReadAllLines(filePath);
        foreach (var line in lines)
        {
            if (line.StartsWith("wavy_id")) continue;
            var parts = line.Split(',');
            if (parts.Length >= 2)
            {
                wavys[parts[0].Trim()] = parts[1].Trim().ToLower();
            }
        }
        return wavys;
    }

    // Carregar regras de roteamento
    static Dictionary<string, RoutingRule> LoadRoutingRules(string filePath)
    {
        var rules = new Dictionary<string, RoutingRule>();
        if (!File.Exists(filePath)) return rules;

        var lines = File.ReadAllLines(filePath);
        foreach (var line in lines)
        {
            if (line.StartsWith("wavy_id")) continue;
            var parts = line.Split(',');
            if (parts.Length >= 5)
            {
                string key = $"{parts[0].Trim()}|{parts[1].Trim()}";
                rules[key] = new RoutingRule
                {
                    Preprocess = parts[2].Trim().ToLower() == "true",
                    ServerIp = parts[3].Trim(),
                    ServerPort = int.Parse(parts[4].Trim())
                };
            }
        }
        return rules;
    }

    // Validar dados TEMP
    static bool Preprocess(string type, string value)
    {
        if (type == "TEMP" && double.TryParse(value, out double temp))
        {
            return temp >= 0 && temp <= 40;
        }
        return true;
    }


}
