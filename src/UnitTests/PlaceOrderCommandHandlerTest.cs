using System;
using System.Collections.Generic;
using System.Linq;
using Domain;
using Endpoint2;
using Endpoint2.Commands;
using EntityFramework.FakeItEasy;
using Moq;
using NServiceBus.Testing;
using NUnit.Framework;
using Order = Domain.Order;

namespace UnitTests
{
    [TestFixture]
    public class PlaceOrderCommandHandlerTest
    {
        private PlaceOrderCommandHandler handler;
        private readonly List<Order> orders = new List<Order>();
        private DateTime placedAtDate;
        private Guid orderId;

        [TestFixtureSetUp]
        public async void Setup()
        {
            var dbContext = new OrderDbContext()
            {
                Orders = Aef.FakeDbSet(orders)
            };

            placedAtDate = DateTime.UtcNow;
            orderId = new Guid("DE81F6B5-7F29-4AE7-A72B-023F6B58DE72");
            var placeOrderCommand = new PlaceOrderCommand
            {
                OrderId = orderId,
                OrderNumber = 100,
                PlacedAtDate = placedAtDate
            };

            var context = new TestableMessageHandlerContext();

            //TODO: Question? How do you test this, since I cannot inject the OrderDbContext into the handler
            //In the handler there is the context.SynchronizedStorageSession.FromCurrentSession() that returns a new OrderDbContext
            //In V5 we did new PlaceOrderCommandHandler(OrderDbContext dbContext); and then we could use the FakeDbSet. 
            //Since I could not use Moq to mock the return value of context.SynchronizedStorageSession.FromCurrentSession() since it is an extension method
            //so I created a OrderStorageContext instead
            //Then I realized that U guys have your own TestingFramework that I installed so that I could get a TestableMessageHandlerContext
            //but cannot find a way to set the session.SqlPersistenceSession();

            var orderStorageContextMock = new Mock<IOrderStorageContext>();
            orderStorageContextMock.SetupIgnoreArgs(x => x.GetOrderDbContext(null)).Returns(dbContext);
        

            handler = new PlaceOrderCommandHandler(orderStorageContextMock.Object);

            try
            {
                await handler.Handle(placeOrderCommand, context)
                    .ConfigureAwait(false);
                await dbContext.SaveChangesAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
           

        }

        [Test]
        public void Order_Should_Be_Placed_at_correct_date()
        {
            Assert.AreEqual(orders.Single(x => x.OrderId == orderId).PlacedAtDate, placedAtDate);
        }
    }
}
