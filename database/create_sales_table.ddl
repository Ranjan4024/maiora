CREATE TABLE sales_data (
    OrderId VARCHAR(255) PRIMARY KEY,
    OrderItemId VARCHAR(255),
    QuantityOrdered INT,
    ItemPrice DOUBLE,
    PromotionDiscount DOUBLE,
    batch_id INT,
    region CHAR(1),
    date DATE,
    total_sales DOUBLE,
    net_sale DOUBLE
);
