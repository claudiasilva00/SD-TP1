using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Aggregator
{
    static void Main()
    {
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
            string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            Console.WriteLine($"WAVY Sent: {message}");

            if (message.StartsWith("REGISTER") || message.StartsWith("DATA"))
            {
                ForwardToServer($"FORWARD {message}", wavyStream);
            }
            else if (message == "QUIT")
            {
                ForwardToServer($"FORWARD QUIT", wavyStream);
                break;
            }
        }

        wavyClient.Close();
    }

    static void ForwardToServer(string message, NetworkStream wavyStream)
    {
        TcpClient serverClient = new TcpClient("127.0.0.1", 5001);
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
}
