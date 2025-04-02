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

    static void Main()
    {
        wavyStates = LoadWavyStates("waves.csv");
        routingRules = LoadRoutingRules("routing.csv");

        TcpListener listener = new TcpListener(IPAddress.Any, 5000);
        listener.Start();
        Console.WriteLine("Aggregator started, waiting for WAVY devices...");

        while (true)
        {
            TcpClient wavyClient = listener.AcceptTcpClient();
            Thread t = new Thread(() => HandleWavy(wavyClient));
            t.Start();
        }
    }


    static void HandleWavy(TcpClient wavyClient)
    {
        NetworkStream wavyStream = wavyClient.GetStream();
        byte[] buffer = new byte[1024];
        int bytesRead;

        while ((bytesRead = wavyStream.Read(buffer, 0, buffer.Length)) > 0)
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
                        Console.WriteLine($"❌ WAVY {wavyId} não está associada.");
                        SendResponseToWavy(wavyStream, "403 NOT ASSOCIATED");
                        continue;
                    }

                    if (wavyStates[wavyId] != "operacao")
                    {
                        Console.WriteLine($"⚠️ WAVY {wavyId} está em estado '{wavyStates[wavyId]}', operação não permitida.");
                        SendResponseToWavy(wavyStream, $"403 BLOCKED STATE: {wavyStates[wavyId]}");
                        continue;
                    }

                    ForwardToServer($"FORWARD {message}", wavyStream, "127.0.0.1", 5001); // SERVER FIXO PARA REGISTER
                }
            }
            else if (message.StartsWith("DATA"))
            {
                string[] parts = message.Split(' ');
                if (parts.Length >= 4)
                {
                    string wavyId = parts[1].Trim();
                    string dataType = parts[2].Trim();
                    string value = parts[3].Trim();

                    string key = $"{wavyId}|{dataType}";
                    if (!routingRules.ContainsKey(key))
                    {
                        Console.WriteLine($"❌ Não há regra de encaminhamento para {key}.");
                        SendResponseToWavy(wavyStream, "404 ROUTING NOT FOUND");
                        continue;
                    }

                    var rule = routingRules[key];

                    if (rule.Preprocess && !Preprocess(dataType, value))
                    {
                        Console.WriteLine($"⚠️ Pré-processamento falhou para {key} com valor {value}.");
                        SendResponseToWavy(wavyStream, "422 PREPROCESSING FAILED");
                        continue;
                    }

                    ForwardToServer($"FORWARD {message}", wavyStream, rule.ServerIp, rule.ServerPort);
                }
            }


        }

        wavyClient.Close();
    }

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

        SendResponseToWavy(wavyStream, response);
        serverClient.Close();
    }

    static void SendResponseToWavy(NetworkStream wavyStream, string response)
    {
        byte[] responseData = Encoding.UTF8.GetBytes(response);
        wavyStream.Write(responseData, 0, responseData.Length);
        Console.WriteLine($"Aggregator Sent to WAVY: {response}");
    }
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
    static bool Preprocess(string type, string value)
    {
        Console.WriteLine($"🧪 [DEBUG] Entrou no Preprocess com type={type}, value={value}");

        if (type == "TEMP")
        {
            if (double.TryParse(value, out double temp))
            {
                bool result = temp >= 0 && temp <= 40;
                Console.WriteLine($"🧪 Resultado TEMP: {result}");
                return result;
            }
            else
            {
                Console.WriteLine($"⚠️ Valor TEMP inválido: {value}");
                return false;
            }
        }

        return true;
    }


}





