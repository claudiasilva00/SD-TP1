using System;
using System.Net.Sockets;
using System.Text;
using System.Timers;

class Wavy
{
    static TcpClient client;
    static NetworkStream stream;
    static System.Timers.Timer dataTimer;
    static Random random = new Random(); // Instância para gerar números aleatórios
    static string wavyId;  // Variável para armazenar o ID do WAVY após o registro

    static void Main()
    {
        string aggregatorIp = "127.0.0.1";  // Endereço do agregador (pode ser alterado conforme necessário)
        int aggregatorPort = 5000;  // Porta do agregador

        // Inicializa a conexão TCP com o Agregador
        client = new TcpClient(aggregatorIp, aggregatorPort);
        stream = client.GetStream();

        Console.WriteLine("Conectado ao Agregador. Digite o comando REGISTER {wavy_id} para registrar o WAVY.");

        // Comando de registro manual
        while (true)
        {
            Console.Write("Comando: ");
            string command = Console.ReadLine()?.Trim();

            if (command.StartsWith("REGISTER"))
            {
                // Envia o comando REGISTER ao Agregador
                RegisterWavy(command);
            }
            else if (command == "QUIT")
            {
                Console.WriteLine("Saindo...");
                // Enviar uma mensagem de saída para o Agregador
                SendMessage(stream, "QUIT");
                // Parar o temporizador e fechar a conexão
                dataTimer?.Stop();  // Garante que o temporizador seja parado antes de encerrar
                client.Close();
                break;
            }
            else
            {
                Console.WriteLine("Comando inválido. Use REGISTER {wavy_id} para registrar.");
            }
        }
    }

    static void RegisterWavy(string command)
    {
        // Extraímos o wavy_id do comando REGISTER
        string[] parts = command.Split(' ');
        if (parts.Length >= 2)
        {
            wavyId = parts[1].Trim();  // Armazena o wavy_id para usar posteriormente

            // Envia o comando REGISTER para o Agregador
            string registerMessage = $"REGISTER {wavyId}";
            SendMessage(stream, registerMessage);  // Corrigido para passar o stream

            string response = ReceiveMessage();
            Console.WriteLine($"Resposta do Agregador: {response}");

            // Se o registro for bem-sucedido, começa o envio de dados periodicamente
            if (response.StartsWith("ACK REGISTERED"))
            {
                Console.WriteLine($"WAVY {wavyId} registrado com sucesso!");
                StartSendingData();
            }
            else
            {
                Console.WriteLine($"Falha no registro de {wavyId}: {response}");
            }
        }
        else
        {
            Console.WriteLine("Comando REGISTER inválido. Utilize REGISTER {wavy_id}.");
        }
    }

    static void StartSendingData()
    {
        // Configura o temporizador para enviar dados a cada 1 segundos
        dataTimer = new System.Timers.Timer(1000); // Envia dados a cada 1 segundo
        dataTimer.Elapsed += (sender, e) => SendData(); // Chama o método SendData ao final de cada intervalo
        dataTimer.Start();

        Console.WriteLine("Começando a enviar dados TEMP periodicamente...");
    }

    static void SendData()
    {
        if (string.IsNullOrEmpty(wavyId))
        {
            Console.WriteLine("Erro: WAVY não registrada. Envio de dados não possível.");
            return;
        }

        // Gerar dados do tipo TEMP com valor aleatório
        string dataType = "TEMP";
        string value = random.Next(-10, 41).ToString();  // Gera um valor aleatório de temperatura entre -10 e 40

        // Formando a mensagem com os dados do tipo TEMP
        string message = $"DATA {wavyId} {dataType} {value}";

        // Enviar os dados para o Agregador
        SendMessage(stream, message);  // Corrigido para passar o stream

        Console.WriteLine($"Dados enviados ao Agregador: {message}");
    }

    static void SendMessage(NetworkStream stream, string message)
    {
        byte[] data = Encoding.UTF8.GetBytes(message);
        stream.Write(data, 0, data.Length);
        Console.WriteLine($"WAVY Sent: {message}");
    }

    static string ReceiveMessage()
    {
        byte[] buffer = new byte[1024];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);
        return Encoding.UTF8.GetString(buffer, 0, bytesRead);
    }
}
