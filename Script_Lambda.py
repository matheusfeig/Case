import csv
import json
import os
import boto3

def lambda_handler(event, context):
    # Recebe o JSON do evento
    json_data = event['Records'][0]['dynamodb']['NewImage']
    
    # Mapeia os campos do JSON para colunas CSV
    csv_data = {
        'merchantId': json_data['merchantId']['S'],
        'customerId': json_data['customerId']['S'],
        'status': json_data['status']['S'],
        'shopper_firstName': json_data['shopper']['M']['firstName']['S'],
        'shopper_lastName': json_data['shopper']['M']['lastName']['S'],
        'shopper_phone': json_data['shopper']['M']['phone']['S'],
        'shopper_billingAddress_id': json_data['shopper']['M']['billingAddress']['M']['id']['S'],
        'shopper_billingAddress_number': json_data['shopper']['M']['billingAddress']['M']['number']['S'],
        'shopper_billingAddress_zipCode': json_data['shopper']['M']['billingAddress']['M']['zipCode']['S'],
        'shopper_billingAddress_phoneNumber': json_data['shopper']['M']['billingAddress']['M']['phoneNumber']['S'],
        'shopper_billingAddress_city': json_data['shopper']['M']['billingAddress']['M']['city']['S'],
        'shopper_billingAddress_street': json_data['shopper']['M']['billingAddress']['M']['street']['S'],
        'shopper_billingAddress_state': json_data['shopper']['M']['billingAddress']['M']['state']['S'],
        'shopper_billingAddress_timestamp': json_data['shopper']['M']['billingAddress']['M']['timestamp']['S'],
        'shopper_id': json_data['shopper']['M']['id']['S'],
        'shopper_birthDate': json_data['shopper']['M']['birthDate']['S'],
        'shopper_email': json_data['shopper']['M']['email']['S'],
        'shopper_timestamp': json_data['shopper']['M']['timestamp']['S'],
        'order_id': json_data['order']['M']['id']['S'],
        'order_reference': json_data['order']['M']['reference']['S'],
        'order_orderAmount': json_data['order']['M']['orderAmount']['N'],
        'order_description': json_data['order']['M']['description']['S'],
        'order_taxAmount': json_data['order']['M']['taxAmount']['N'],
        'order_timestamp': json_data['order']['M']['timestamp']['S'],
        'order_items_id': [],
        'order_items_reference': [],
        'order_items_image': [],
        'order_items_quantity': [],
        'order_items_price': [],
        'order_items_name': [],
        'order_items_sku': [],
        'order_items_url': []
    }
    
    # Mapeia os itens da ordem
    for item in json_data['order']['M']['items']['L']:
        csv_data['order_items_id'].append(item['M']['id']['S'])
        csv_data['order_items_reference'].append(item['M']['reference']['S'])
        csv_data['order_items_image'].append(item['M']['image']['S'])
        csv_data['order_items_quantity'].append(item['M']['quantity']['N'])
        csv_data['order_items_price'].append(item['M']['price']['N'])
        csv_data['order_items_name'].append(item['M']['name']['S'])
        csv_data['order_items_sku'].append(item['M']['sku']['S'])
        csv_data['order_items_url'].append(item['M']['url']['S'])
    
    # Nome do arquivo CSV de saída
    output_filename = '/output_order.csv'
    
    # Escreve os dados CSV em um arquivo
    with open(output_filename, 'w', newline='') as csvfile:
        fieldnames = csv_data.keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(csv_data)
    
    # Envia o arquivo CSV para o Amazon S3
    s3 = boto3.client('s3')
    s3_bucket_name = 'bucket_1'
    s3_key = 'order_list/output_order.csv'
    s3.upload_file(output_filename, s3_bucket_name, s3_key)
    
    # Retorna a localização do arquivo no S3 para fins de registro
    return {
        'statusCode': 200,
        'body': f'Arquivo CSV enviado para s3://{s3_bucket_name}/{s3_key}'
    }