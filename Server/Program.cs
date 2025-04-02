using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Server
{
    static void Main()
    {
        TcpListener listener = new TcpListener(IPAddress.Any, 5001);
        listener.Start();
        Console.WriteLine("?? Server started, waiting for connections...");

        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            Thread t = new Thread(() => HandleClient(client));
            t.Start();
        }
    }

    static void HandleClient(TcpClient client)
    {
        NetworkStream stream = client.GetStream();
        byte[] buffer = new byte[1024];

        while (true)
        {
            int bytesRead = stream.Read(buffer, 0, buffer.Length);
            if (bytesRead == 0) break;

            string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            Console.WriteLine($"Received: {message}");

            if (message.StartsWith("FORWARD REGISTER"))
            {
                byte[] response = Encoding.UTF8.GetBytes("ACK REGISTERED");
                stream.Write(response, 0, response.Length);
            }
            else if (message.StartsWith("FORWARD DATA"))
            {
                byte[] response = Encoding.UTF8.GetBytes("100 OK");
                stream.Write(response, 0, response.Length);
            }
            else if (message.StartsWith("FORWARD QUIT"))
            {
                byte[] response = Encoding.UTF8.GetBytes("400 BYE");
                stream.Write(response, 0, response.Length);
                Console.WriteLine("?? Server encerrando conexão com WAVY.");
                break;
            }
        }

        client.Close();
    }
}
