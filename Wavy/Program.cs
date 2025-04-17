using System;
using System.Net.Sockets;
using System.Text;
using System.Timers;
using System.Collections.Generic;

class Wavy
{
    static TcpClient client;
    static NetworkStream stream;
    static System.Timers.Timer dataTimer;
    static System.Timers.Timer dataCollectionTimer;  // Para coleta de dados a cada 10 segundos
    static System.Timers.Timer dataSendTimer;
    static Random random = new Random(); // Instância para gerar números aleatórios
    static string wavyId;  // Variável para armazenar o ID do WAVY após o registro
    static List<string> dataBuffer = new List<string>();

    // Flag para controlar se os dados estão sendo gerados
    static bool isGeneratingData = true;

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
                dataCollectionTimer?.Stop();
                dataSendTimer?.Stop();
                break;
            }
            else if (command == "STOP DATA")
            {
                StopDataGeneration(); // Comando para parar a geração de dados
            }
            else
            {
                Console.WriteLine("Comando inválido. Use REGISTER {wavy_id} para registrar.");
            }
        }

        // Fechar a conexão corretamente após o comando "QUIT"
        client.Close();
        Console.WriteLine("Conexão encerrada.");
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
                StartDataCollection();
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

    // Comando para parar a geração de dados
    static void StopDataGeneration()
    {
        isGeneratingData = false;
        Console.WriteLine("Geração de dados parada.");
    }

    static void StartDataCollection()
    {
        // Temporizador para coletar dados a cada 10 segundos
        dataCollectionTimer = new System.Timers.Timer(3000);
        dataCollectionTimer.Elapsed += (sender, e) => CollectData();
        dataCollectionTimer.Start();

        // Temporizador para enviar dados ao Agregador a cada 30 segundos
        dataSendTimer = new System.Timers.Timer(3000);
        dataSendTimer.Elapsed += (sender, e) => SendData();
        dataSendTimer.Start();

        Console.WriteLine("Começando a coletar e enviar dados TEMP periodicamente...");
    }

    static void CollectData()
    {
        // Se a flag isGeneratingData estiver desativada, não gera dados
        if (!isGeneratingData)
        {
            return;  // Interrompe a coleta de dados
        }

        // Gerar dados do tipo TEMP com valor aleatório
        string dataType = "TEMP";
        string value = random.Next(-10, 41).ToString();  // Gera um valor aleatório de temperatura entre -10 e 40

        // Formando a mensagem com os dados do tipo TEMP
        string message = $"{wavyId} {dataType} {value}";

        // Armazenar o dado coletado
        dataBuffer.Add(message);
        Console.WriteLine($"Dado coletado: {message}");
    }

    static void SendData()
    {
        if (dataBuffer.Count == 0)
        {
            Console.WriteLine("Nenhum dado para enviar.");
            return;
        }

        // Formando a mensagem com os dados acumulados
        string allData = string.Join("\n", dataBuffer);
        string message = $"DATA {wavyId} {allData}";

        // Enviar os dados para o Agregador
        SendMessage(stream, message);
        Console.WriteLine($"Dados enviados ao Agregador: {message}");

        // Limpa o buffer após o envio
        dataBuffer.Clear();
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
