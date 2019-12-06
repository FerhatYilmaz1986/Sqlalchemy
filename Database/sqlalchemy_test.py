from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric, SmallInteger 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship 
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from pprint import pprint
from sqlalchemy import or_ 
from sqlalchemy import and_
from sqlalchemy import not_
engine = create_engine('sqlite:///example.db')
Base = declarative_base()
 
class OrderLine(Base):
    __tablename__ = 'order_lines'
    id = Column(Integer(), primary_key=True)
    order_id = Column(Integer(), ForeignKey('orders.id'))
    item_id = Column(Integer(), ForeignKey('items.id'))
    quantity = Column(Integer())
    order = relationship("Order", backref='order_lines')
    item = relationship("Item")


class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer(), primary_key=True)
    customer_id = Column(Integer(), ForeignKey('customers.id'))
    date_placed = Column(DateTime(), default=datetime.now, nullable=False)
    date_shipped = Column(DateTime())

class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer(), primary_key=True)
    name = Column(String(200), nullable=False)
    cost_price = Column(Numeric(10, 2), nullable=False)
    selling_price = Column(Numeric(10, 2), nullable=False)
    quantity = Column(Integer(), nullable=False)
    
class Person(Base):
    __tablename__ = 'person'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)

class Address(Base):
    __tablename__ = 'address'
    # Here we define columns for the table address.
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    street_name = Column(String(250))
    street_number = Column(String(250))
    post_code = Column(String(250), nullable=False)
    person_id = Column(Integer, ForeignKey('person.id'))
    person = relationship("Person", backref="address")
    
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer(), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    username = Column(String(50), nullable=False)
    email = Column(String(200), nullable=False)
    address = Column(String(200), nullable=False)
    town = Column(String(50), nullable=False)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)
    orders = relationship("Order", backref='customer')
    
def dispatch_order(order_id):
    # check whether order_id is valid or not
    order = session.query(Order).get(order_id)

    if not order:
        raise ValueError("Invalid order id: {}.".format(order_id))

    if order.date_shipped:
        print("Order already shipped.")
        return

    try:
        for i in order.order_lines:
            i.item.quantity = i.item.quantity - i.quantity

        order.date_shipped = datetime.now()
        session.commit()
        print("Transaction completed.")

    except IntegrityError as e:
        print(e)
        print("Rolling back ...")
        session.rollback()
        print("Transaction failed.")
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

#Add customer data
c1 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com',
              address='1662 Kinney Street',
              town='Wolfden'
              )

c2 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com',
              address='424 Patterson Street',
              town='Beckinsdale'
              )

session.add(c1)
session.add(c2)
session.new
session.commit()
c1.first_name, c1.last_name
c2.first_name, c2.last_name
c1.id
c3 = Customer(
    first_name="John",
    last_name="Lara",
    username="johnlara",
    email="johnlara@mail.com",
    address="3073 Derek Drive",
    town="Norfolk"
)

c4 = Customer(
    first_name="Sarah",
    last_name="Tomlin",
    username="sarahtomlin",
    email="sarahtomlin@mail.com",
    address="3572 Poplar Avenue",
    town="Norfolk"
)

c5 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com',
              address='1662 Kinney Street',
              town='Wolfden'
              )

c6 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com',
              address='424 Patterson Street',
              town='Beckinsdale'
              )

session.add_all([c3, c4, c5, c6])
session.commit()

#Add order data
o1 = Order(customer=c1)
o2 = Order(customer=c1)

line_item1 = OrderLine(order=o1, item=i1, quantity=3)
line_item2 = OrderLine(order=o1, item=i2, quantity=2)
line_item3 = OrderLine(order=o2, item=i1, quantity=1)
line_item3 = OrderLine(order=o2, item=i2, quantity=4)

session.add_all([o1, o2])

session.new
session.commit()
o3 = Order(customer=c1)
orderline1 = OrderLine(item=i1, quantity=5)
orderline2 = OrderLine(item=i2, quantity=10)

o3.order_lines.append(orderline1)
o3.order_lines.append(orderline2)

session.add_all([o3])

session.commit()
c1.orders
o1.customer
for ol in c1.orders[0].order_lines:
    ol.id, ol.item, ol.quantity

print('-------')

for ol in c1.orders[1].order_lines:
    ol.id, ol.item, ol.quantity
    

session.query(Item).filter(Item.name.ilike("wa%")).all()
session.query(Item).filter(Item.name.ilike("wa%")).order_by(Item.cost_price).all()

#Querrying Data
session.query(Customer).all()
session.query(Item).all()
session.query(Order).all()
print(session.query(Customer))
q = session.query(Customer)
 
for c in q:
    print(c.id, c.first_name)
session.query(Customer.id, Customer.first_name).all()

#Count method
session.query(Customer).count() # get the total number of records in the customers table
session.query(Item).count()  # get the total number of records in the items table
session.query(Order).count()  # get the total number of records in the orders table
#First method
session.query(Customer).first()
session.query(Item).first()
session.query(Order).first()
#Get method
session.query(Customer).get(1)
session.query(Item).get(1)
session.query(Order).get(100)
#Filter method
session.query(Customer).filter(Customer.first_name == 'John').all()
print(session.query(Customer).filter(Customer.first_name == 'John'))
session.query(Customer).filter(Customer.id <= 5, Customer.town == "Norfolk").all()
# find all customers who either live in Peterbrugh or Norfolk
session.query(Customer).filter(or_(
    Customer.town == 'Peterbrugh', 
    Customer.town == 'Norfolk'
)).all()
 
 
# find all customers whose first name is John and live in Norfolk
 
session.query(Customer).filter(and_(
    Customer.first_name == 'John', 
    Customer.town == 'Norfolk'
)).all()
 
 
# find all johns who don't live in Peterbrugh
 
session.query(Customer).filter(and_(
    Customer.first_name == 'John', 
    not_(
        Customer.town == 'Peterbrugh', 
    )
)).all()
# IS NULL
session.query(Order).filter(Order.date_shipped == None).all()
# IS NOT NULL
session.query(Order).filter(Order.date_shipped != None).all()
#IN
session.query(Customer).filter(Customer.first_name.in_(['Toby', 'Sarah'])).all()
#NOT IN
session.query(Customer).filter(Customer.first_name.notin_(['Toby', 'Sarah'])).all()
#BETWEEN
session.query(Item).filter(Item.cost_price.between(10, 50)).all()
#NOT BETWEEN
session.query(Item).filter(not_(Item.cost_price.between(10, 50))).all()
#LIKE
session.query(Item).filter(Item.name.like("%r")).all()
#NOT LIKE
session.query(Item).filter(not_(Item.name.like("W%"))).all()
#LIMIT
session.query(Customer).limit(2).all()
session.query(Customer).filter(Customer.address.ilike("%avenue")).limit(2).all()
print(session.query(Customer).limit(2))
print(session.query(Customer).filter(Customer.address.ilike("%avenue")).limit(2))
#OFFSET
session.query(Customer).limit(2).offset(2).all()
print(session.query(Customer).limit(2).offset(2))
#ORDER BY
session.query(Item).filter(Item.name.ilike("wa%")).all()
session.query(Item).filter(Item.name.ilike("wa%")).order_by(Item.cost_price).all()
#JOIN
session.query(Customer).join(Order).all()
print(session.query(Customer).join(Order))
session.query(
    Customer.first_name, 
    Item.name, 
    Item.selling_price, 
    OrderLine.quantity
).join(Order).join(OrderLine).join(Item).filter(
    Customer.first_name == 'John',
    Customer.last_name == 'Green',
    Order.id == 1,
).all()
#OUTER JOIN (full outer join is not supported)
session.query(        
    Customer.first_name,
    Order.id,
).outerjoin(Order).all()
#GROUP BY
from sqlalchemy import func
 
session.query(func.count(Customer.id)).join(Order).filter(
    Customer.first_name == 'John',
    Customer.last_name == 'Green',    
).group_by(Customer.id).scalar()
#HAVING
session.query(
    func.count("*").label('town_count'),    
    Customer.town
).group_by(Customer.town).having(func.count("*") > 2).all()
#DUPLICATES
from sqlalchemy import distinct
 
session.query(Customer.town).filter(Customer.id  < 10).all()
session.query(Customer.town).filter(Customer.id  < 10).distinct().all()
 
session.query(        
    func.count(distinct(Customer.town)),
    func.count(Customer.town)
).all()
#UNIONS
s1 = session.query(Item.id, Item.name).filter(Item.name.like("Wa%"))
s2 = session.query(Item.id, Item.name).filter(Item.name.like("%e%"))
s1.union(s2).all()
#UPDATING DATA
i = session.query(Item).get(8)
i.selling_price = 25.91
session.add(i)
session.commit()
#DELETING DATA
i = session.query(Item).filter(Item.name == 'Monitor').one()
i
session.delete(i)
session.commit()
session.query(Item).filter(
    Item.name.ilike("W%")
).delete(synchronize_session='fetch')
session.commit()
#RAW QUERIES
from sqlalchemy import text
 
session.query(Customer).filter(text("first_name = 'John'")).all()
 
session.query(Customer).filter(text("town like 'Nor%'")).all()
 
session.query(Customer).filter(text("town like 'Nor%'")).order_by(text("first_name, id desc")).all()
#TRANSACTIONS
dispatch_order(1)
dispatch_order(2)
