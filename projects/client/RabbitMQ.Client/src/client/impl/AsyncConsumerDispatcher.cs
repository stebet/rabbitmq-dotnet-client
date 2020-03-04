using System;
using System.Threading.Tasks;

namespace RabbitMQ.Client.Impl
{
    internal sealed class AsyncConsumerDispatcher : IConsumerDispatcher
    {
        private readonly ModelBase _model;
        private readonly AsyncConsumerWorkService _workService;

        public AsyncConsumerDispatcher(ModelBase model, AsyncConsumerWorkService ws)
        {
            _model = model;
            _workService = ws;
            IsShutdown = false;
        }

        public void Quiesce()
        {
            IsShutdown = true;
        }

        public void Shutdown()
        {
            _workService.Stop();
        }

        public void Shutdown(IModel model)
        {
            _workService.Stop(model);
        }

        public bool IsShutdown
        {
            get;
            private set;
        }

        public Task HandleBasicConsumeOk(IBasicConsumer consumer,
            string consumerTag)
        {
            return Schedule(new BasicConsumeOk(consumer, consumerTag));
        }

        public Task HandleBasicDeliver(IBasicConsumer consumer,
            string consumerTag,
            ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            IBasicProperties basicProperties,
            ReadOnlyMemory<byte> body)
        {
            return Schedule(new BasicDeliver(consumer, consumerTag, deliveryTag, redelivered, exchange, routingKey, basicProperties, body));
        }

        public Task HandleBasicCancelOk(IBasicConsumer consumer, string consumerTag)
        {
            return Schedule(new BasicCancelOk(consumer, consumerTag));
        }

        public Task HandleBasicCancel(IBasicConsumer consumer, string consumerTag)
        {
            return Schedule(new BasicCancel(consumer, consumerTag));
        }

        public Task HandleModelShutdown(IBasicConsumer consumer, ShutdownEventArgs reason)
        {
            return new ModelShutdown(consumer, reason).Execute(_model);
        }

        private Task Schedule<TWork>(TWork work) where TWork : Work
        {
            if (!IsShutdown)
            {
                return work.Execute(_model);
            }

            return Task.CompletedTask;
        }
    }
}
