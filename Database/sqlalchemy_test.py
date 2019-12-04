from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pprint import pprint
from datetime import datetime
from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy import cast, Date, distinct, union
from sqlalchemy import distinct
from sqlalchemy import desc
# Create an engine that stores data in the local directory's
# sqlalchemy_example.db file.
engine = create_engine('sqlite:////web/Sqlite-Data/example.db')

# this loads the sqlalchemy base class
Base = declarative_base()


# Setting up the classes that create the record objects and define the schema

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
    # creates the field to store the person id
    person_id = Column(Integer, ForeignKey('person.id'))
    # creates the relationship between the person and addresses.  backref adds a property to the Person class to retrieve addresses
    person = relationship("Person", backref="addresses")
class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer(), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    username = Column(String(50), nullable=False)
    email = Column(String(200), nullable=False)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)
    orders = relationship("Order", backref='customer')


class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer(), primary_key=True)
    name = Column(String(200), nullable=False)
    cost_price =  Column(Numeric(10, 2), nullable=False)
    selling_price = Column(Numeric(10, 2),  nullable=False)
    #orders = relationship("Order", backref='customer')

class OrderLine(Base):
    __tablename__ = 'order_lines'
    id = Column(Integer(), primary_key=True)
    order_id = Column(Integer(), ForeignKey('orders.id'))
    item_id = Column(Integer(), ForeignKey('items.id'))
    quantity = Column(SmallInteger())
    item = relationship("Item")

class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer(), primary_key=True)
    customer_id = Column(Integer(), ForeignKey('customers.id'))
    date_placed = Column(DateTime(), default=datetime.now)

# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)

Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
# A DBSession() instance establishes all conversations with the database
# and represents a "staging zone" for all the objects loaded into the
# database session object. Any change made against the objects in the
# session won't be persisted into the database until you call
# session.commit(). If you're not happy about the changes, you can
# revert all of them back to the last commit by calling
# session.rollback()
session = DBSession()
#Insert customers:
c1 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com')
session.add(c1)
c2 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com')
session.add(c2)
session.commit()
#Insert more customers
c3 = Customer(
    first_name="John",
    last_name="Lara",
    username="johnlara",
    email="johnlara@mail.com")

c4 = Customer(
    first_name="Sarah",
    last_name="Tomlin",
    username="sarahtomlin",
    email="sarahtomlin@mail.com")

c5 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com')

c6 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com')

session.add_all([c3, c4, c5, c6])
session.commit()

#Create some products
i1 = Item(name='Chair', cost_price=9.21, selling_price=10.81)
i2 = Item(name='Pen', cost_price=3.45, selling_price=4.51)
i3 = Item(name='Headphone', cost_price=15.52, selling_price=16.81)
i4 = Item(name='Travel Bag', cost_price=20.1, selling_price=24.21)
i5 = Item(name='Keyboard', cost_price=20.1, selling_price=22.11)
i6 = Item(name='Monitor', cost_price=200.14, selling_price=212.89)
i7 = Item(name='Watch', cost_price=100.58, selling_price=104.41)
i8 = Item(name='Water Bottle', cost_price=20.89, selling_price=25)

session.add_all([i1, i2, i3, i4, i5, i6, i7, i8])
session.commit()


# Insert a Person in the person table
new_person1 = Person(name='Keith')
session.add(new_person1)

new_person2 = Person(name='Joe')
session.add(new_person1)

new_person3 = Person(name='Steve')
session.add(new_person1)
session.commit()

# Insert an Address in the address table using a loop

addresses = [
    Address(post_code='00001', person=new_person1),
    Address(post_code='00002', person=new_person2),
    Address(post_code='00003', person=new_person3),
]

# Loop through addresses and commit them to the database
for address in addresses:
    session.add(address)
    session.commit()

# joins Person on Address
all_people = session.query(Person).join(Address).all()

# Accessing a person with their address, You have to loop the addresses property and remember it was added by the
# backref on the addresses class
for person in all_people:
    # use the __dict__ magic method to have the object print it's properties
    pprint(person.__dict__)
    for address in person.addresses:
        pprint(address.__dict__)

# Retrieving the inverse of the relationship.  Notice I reverse the Person and Address to load the Address table
all_addresses = session.query(Address).join(Person).all()
for address in all_addresses:
    # showing how to use the print function with printing text and data at the same time easily
    print(f'{address.person.name} has a postal code of {address.post_code}')

#Query data
print(session.query(Customer))
q = session.query(Customer)

for c in q:
    print(c.id, c.first_name)

session.query(Customer.id, Customer.first_name).all()

session.query(Customer).count()
session.query(Item).count()
session.query(Order).count()

session.query(Customer).first()
session.query(Item).first()
session.query(Order).first()

session.query(Customer).get(1)
session.query(Item).get(1)
session.query(Order).get(100)

session.query(Customer).filter(Customer.first_name == 'John').all()
print(session.query(Customer).filter(Customer.first_name == 'John'))
session.query(Customer).filter(Customer.id <= 5).all()

session.query(Order).filter(Order.date_shipped == None).all()
session.query(Order).filter(Order.date_shipped != None).all()
session.query(Customer).filter(Customer.first_name.in_(['Toby', 'Sarah'])).all()
session.query(Customer).filter(Customer.first_name.notin_(['Toby', 'Sarah'])).all()
session.query(Item).filter(Item.cost_price.between(10, 50)).all()
session.query(Item).filter(not_(Item.cost_price.between(10, 50))).all()
session.query(Item).filter(Item.name.like("%r")).all()
session.query(Item).filter(Item.name.ilike("w%")).all()
session.query(Item).filter(not_(Item.name.like("W%"))).all()
session.query(Customer).limit(2).all()
session.query(Customer).filter(Customer.address.ilike("%avenue")).limit(2).all()
session.query(Customer).limit(2).offset(2).all()
print(session.query(Customer).limit(2).offset(2))
session.query(Item).filter(Item.name.ilike("wa%")).all()
session.query(Item).filter(Item.name.ilike("wa%")).order_by(Item.cost_price).all()

session.query(Item).filter(Item.name.ilike("wa%")).order_by(desc(Item.cost_price)).all()

session.query(Customer).join(Order).all()
print(session.query(Customer).join(Order))
session.query(Customer.id, Customer.username, Order.id).join(Order).all()

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

session.query(
    Customer.first_name,
    Order.id,
).outerjoin(Order).all()

session.query(
    Customer.first_name,
    Order.id,
).outerjoin(Order, full=True).all()

session.query(func.count(Customer.id)).join(Order).filter(
    Customer.first_name == 'John',
    Customer.last_name == 'Green',
).group_by(Customer.id).scalar()

# find the number of customers lives in each town

session.query(
    func.count("*").label('town_count'),
    Customer.town
).group_by(Customer.town).having(func.count("*") > 2).all()

session.query(Customer.town).filter(Customer.id < 10).all()
session.query(Customer.town).filter(Customer.id < 10).distinct().all()

session.query(
    func.count(distinct(Customer.town)),
    func.count(Customer.town)
).all()

session.query(
    cast(func.pi(), Integer),
    cast(func.pi(), Numeric(10, 2)),
    cast("2010-12-01", DateTime),
    cast("2010-12-01", Date),
).all()

s1 = session.query(Item.id, Item.name).filter(Item.name.like("Wa%"))
s2 = session.query(Item.id, Item.name).filter(Item.name.like("%e%"))
s1.union(s2).all()

s1.union_all(s2).all()

i = session.query(Item).get(8)
i.selling_price = 25.91
session.add(i)
session.commit()

# update quantity of all quantity of items to 60 whose name starts with 'W'

session.query(Item).filter(
    Item.name.ilike("W%")
).update({"quantity": 60}, synchronize_session='fetch')
session.commit()

i = session.query(Item).filter(Item.name == 'Monitor').one()
session.delete(i)
session.commit()

dispatch_order(1)
dispatch_order(2)
