using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:29092" };
        string topicName = "temperatures";

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        Random random = new Random();

        while (true)
        {
            int temperature = random.Next(0, 41);
            await producer.ProduceAsync(topicName, new Message<Null, string> { Value = temperature.ToString() });
            Console.WriteLine($"Temperatura enviada: {temperature} °C");
            await Task.Delay(3000);
        }
    }
}
