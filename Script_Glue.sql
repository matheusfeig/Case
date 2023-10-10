CREATE TEMPORARY TABLE temp_table 
(
    merchantId STRING,
    customerId STRING,
    status STRING,
    shopper_firstName STRING,
    shopper_lastName STRING,
    shopper_phone STRING,
    shopper_billingAddress_id STRING,
    shopper_billingAddress_number STRING,
    shopper_billingAddress_zipCode STRING,
    shopper_billingAddress_phoneNumber STRING,
    shopper_billingAddress_city STRING,
    shopper_billingAddress_street STRING,
    shopper_billingAddress_state STRING,
    shopper_billingAddress_timestamp STRING,
    shopper_id STRING,
    shopper_birthDate STRING,
    shopper_email STRING,
    shopper_timestamp STRING,
    order_id STRING,
    order_reference STRING,
    order_orderAmount DECIMAL(18, 2),
    order_description STRING,
    order_taxAmount DECIMAL(18, 2),
    order_timestamp STRING,
    order_items_id ARRAY<STRING>,
    order_items_reference ARRAY<STRING>,
    order_items_image ARRAY<STRING>,
    order_items_quantity ARRAY<INT>,
    order_items_price ARRAY<DECIMAL(18, 2)>,
    order_items_name ARRAY<STRING>,
    order_items_sku ARRAY<STRING>,
    order_items_url ARRAY<STRING>
);


-- Copia os dados do CSV no S3 para a tabela temporária
COPY temp_table
FROM 's3://bucket_1/order_list/output_order.csv'
CREDENTIALS 'XXXXXX'
CSV
IGNOREHEADER 1
DELIMITER ','
;


-- Tabela para o pedido (order)
CREATE TABLE public.orders (
    id VARCHAR(36),
    merchantId VARCHAR(36),
    customerID VARCHAR(36),
    reference VARCHAR(50),
    orderAmount DECIMAL,
    description VARCHAR(255),
    taxAmount DECIMAL,
    timestamp TIMESTAMP
);

-- Tabela para os itens do pedido
CREATE TABLE public.order_items (
    id VARCHAR(36), 
    order_id VARCHAR(36), --public.order.id
    reference VARCHAR(50),
    image VARCHAR(255),
    quantity INT,
    price DECIMAL,
    name VARCHAR(255),
    sku VARCHAR(50),
    url VARCHAR(255)
);

-- Crie uma tabela para o cliente
CREATE TABLE public.customer (
    id VARCHAR(36), -- public.order.customerID
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone VARCHAR(15),
    billing_address_id VARCHAR(36),
    birth_date DATE,
    email VARCHAR(255),
    timestamp TIMESTAMP
);

-- Crie uma tabela para o endereço de cobrança
CREATE TABLE public.billing_address (
    id VARCHAR(36), public.order.billing_address_id
    number VARCHAR(10),
    zip_code VARCHAR(15),
    phone_number VARCHAR(15),
    city VARCHAR(100),
    street VARCHAR(255),
    state VARCHAR(2),
    timestamp TIMESTAMP
);



-- Inserção dos Campos
INSERT INTO public.orders
SELECT
    order_id,
    order_reference,
    order_orderAmount,
    order_description,
    order_taxAmount,
    TO_TIMESTAMP(order_timestamp, 'YYYY-MM-DDTHH:MI:SS.MSZ')
FROM temp_table;



INSERT INTO public.order_items
SELECT
    unnest(order_items_id),
    unnest(order_id),
    unnest(order_items_reference),
    unnest(order_items_image),
    unnest(order_items_quantity),
    unnest(order_items_price),
    unnest(order_items_name),
    unnest(order_items_sku),
    unnest(order_items_url)
FROM temp_table;



INSERT INTO public.customers
SELECT
    shopper_id,
    shopper_firstName,
    shopper_lastName,
    shopper_phone,
    shopper_billingAddress_id,
    shopper_birthDate,
    shopper_email,
    TO_TIMESTAMP(shopper_timestamp, 'YYYY-MM-DDTHH:MI:SS.MSZ')
FROM temp_table;



INSERT INTO public.billing_addresses
SELECT
    shopper_billingAddress_id,
    shopper_billingAddress_number,
    shopper_billingAddress_zipCode,
    shopper_billingAddress_phoneNumber,
    shopper_billingAddress_city,
    shopper_billingAddress_street,
    shopper_billingAddress_state,
    TO_TIMESTAMP(shopper_billingAddress_timestamp, 'YYYY-MM-DDTHH:MI:SS.MSZ')
FROM temp_table;