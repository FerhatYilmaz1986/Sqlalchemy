from sqlalchemy_test import Order
from sqlalchemy.exc import IntegrityError
from datetime import datetime
 
def dispatch_order(session, order_id):
 
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
