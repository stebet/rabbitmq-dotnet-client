using System;
using System.Threading.Tasks;

namespace RabbitMQ.Client.Impl
{
    internal sealed class AsyncConsumerDispatcher : IConsumerDispatcher
    {
        private readonly ModelBase _model;

        public AsyncConsumerDispatcher(ModelBase model)
        {
            _model = model;
            IsShutdown = false;
        }

        public void Quiesce() => IsShutdown = true;

        public void Shutdown()
        {
        }

        public void Shutdown(IModel model)
        {
        }

        public bool IsShutdown { get; private set; }

        public Task HandleBasicConsumeOk(IBasicConsumer consumer, string consumerTag) => Schedule(new BasicConsumeOk(consumer, consumerTag));

        public Task HandleBasicDeliver(IBasicConsumer consumer, string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties basicProperties, ReadOnlyMemory<byte> body)
            => Schedule(new BasicDeliver(consumer, consumerTag, deliveryTag, redelivered, exchange, routingKey, basicProperties, body));

        public Task HandleBasicCancelOk(IBasicConsumer consumer, string consumerTag) => Schedule(new BasicCancelOk(consumer, consumerTag));

        public Task HandleBasicCancel(IBasicConsumer consumer, string consumerTag) => Schedule(new BasicCancel(consumer, consumerTag));

        public Task HandleModelShutdown(IBasicConsumer consumer, ShutdownEventArgs reason) => new ModelShutdown(consumer, reason).Execute(_model);

        private Task Schedule<TWork>(TWork work) where TWork : Work => IsShutdown ? Task.CompletedTask : work.Execute(_model);
    }
}
