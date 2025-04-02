using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

class Wavy
{
    static void Main()
    {
        string aggregatorIp = "127.0.0.1";
        int aggregatorPort = 5000;
        TcpClient client = new TcpClient(aggregatorIp, aggregatorPort);
        NetworkStream stream = client.GetStream();

        Console.WriteLine("?? Conectado ao Aggregator. Digite comandos:");
        while (true)
        {
            Console.Write("Comando: ");
            string command = Console.ReadLine()?.Trim();

            SendMessage(stream, command);
            string response = ReceiveMessage(stream);
            Console.WriteLine($"Aggregator Response: {response}");

            if (command == "QUIT")
            {
                Console.WriteLine("?? Conexão encerrada.");
                break;
            }
        }

        client.Close();
    }

    static void SendMessage(NetworkStream stream, string message)
    {
        byte[] data = Encoding.UTF8.GetBytes(message);
        stream.Write(data, 0, data.Length);
        Console.WriteLine($"?? WAVY Sent: {message}");
    }

    static string ReceiveMessage(NetworkStream stream)
    {
        byte[] buffer = new byte[1024];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);
        return Encoding.UTF8.GetString(buffer, 0, bytesRead);
    }
}
