using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Impl
{
    internal sealed class ConcurrentConsumerDispatcher : IConsumerDispatcher
    {
        private readonly ModelBase _model;
        private readonly ConsumerWorkService _workService;

        public ConcurrentConsumerDispatcher(ModelBase model, ConsumerWorkService ws)
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
            _workService.StopWork();
        }

        public void Shutdown(IModel model)
        {
            _workService.StopWork(model);
        }

        public bool IsShutdown
        {
            get;
            private set;
        }

        public Task HandleBasicConsumeOk(IBasicConsumer consumer,
                                         string consumerTag)
        {
            return Execute(() =>
            {
                try
                {
                    consumer.HandleBasicConsumeOk(consumerTag);
                }
                catch (Exception e)
                {
                    var details = new Dictionary<string, object>()
                    {
                        {"consumer", consumer},
                        {"context",  "HandleBasicConsumeOk"}
                    };
                    _model.OnCallbackException(CallbackExceptionEventArgs.Build(e, details));
                }
            });
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
            return Execute(() =>
            {
                try
                {
                    consumer.HandleBasicDeliver(consumerTag,
                                                deliveryTag,
                                                redelivered,
                                                exchange,
                                                routingKey,
                                                basicProperties,
                                                body);
                }
                catch (Exception e)
                {
                    var details = new Dictionary<string, object>()
                    {
                        {"consumer", consumer},
                        {"context",  "HandleBasicDeliver"}
                    };
                    _model.OnCallbackException(CallbackExceptionEventArgs.Build(e, details));
                }
            });
        }

        public Task HandleBasicCancelOk(IBasicConsumer consumer, string consumerTag)
        {
            return Execute(() =>
            {
                try
                {
                    consumer.HandleBasicCancelOk(consumerTag);
                }
                catch (Exception e)
                {
                    var details = new Dictionary<string, object>()
                    {
                        {"consumer", consumer},
                        {"context",  "HandleBasicCancelOk"}
                    };
                    _model.OnCallbackException(CallbackExceptionEventArgs.Build(e, details));
                }
            });
        }

        public Task HandleBasicCancel(IBasicConsumer consumer, string consumerTag)
        {
            return Execute(() =>
            {
                try
                {
                    consumer.HandleBasicCancel(consumerTag);
                }
                catch (Exception e)
                {
                    var details = new Dictionary<string, object>()
                    {
                        {"consumer", consumer},
                        {"context",  "HandleBasicCancel"}
                    };
                    _model.OnCallbackException(CallbackExceptionEventArgs.Build(e, details));
                }
            });
        }

        public Task HandleModelShutdown(IBasicConsumer consumer, ShutdownEventArgs reason)
        {
            // the only case where we ignore the shutdown flag.
            try
            {
                consumer.HandleModelShutdown(_model, reason);
            }
            catch (Exception e)
            {
                var details = new Dictionary<string, object>()
                    {
                        {"consumer", consumer},
                        {"context",  "HandleModelShutdown"}
                    };
                _model.OnCallbackException(CallbackExceptionEventArgs.Build(e, details));
            };

            return Task.CompletedTask;
        }

        private Task Execute(Action fn)
        {
            if (!IsShutdown)
            {
                _workService.AddWork(_model, fn);
                return Task.CompletedTask;
            }

            return Task.CompletedTask;
        }
    }
}
