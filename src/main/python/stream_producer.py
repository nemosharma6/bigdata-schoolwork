from kafka import KafkaProducer
# import json
import sys
import requests
# import time


def get_data(u):
    u = u + '&page='
    data = []

    for k in range(1, 80):
        json_data = requests.get(u + str(k)).json()
        for i in range(len(json_data["response"]['results'])):
            headline = json_data["response"]['results'][i]['fields']['headline']
            body_text = json_data["response"]['results'][i]['fields']['bodyText']
            headline += ". "
            headline += body_text
            label = json_data["response"]['results'][i]['sectionName']
            to_add = label + '||' + headline
            data.append(to_add)

    print(len(data))
    return data


def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo'.encode('utf-8'))
        value_bytes = bytes(value.encode('utf-8'))
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        # print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print('Number of arguments is not correct')
        exit()
    
    key = sys.argv[1]
    from_date = sys.argv[2]
    to_date = sys.argv[3]

    url = 'http://content.guardianapis.com/search?from-date=' + from_date + '&to-date=' + to_date + \
          '&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key=' + key

    all_news = get_data(url)
    if len(all_news) > 0:
        prod = connect_kafka_producer()
        for story in all_news:
            # print(json.dumps(story))
            publish_message(prod, 'guardian9', story)
        if prod is not None:
            prod.close()
