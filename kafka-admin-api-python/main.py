import logging
import os
from dotenv import load_dotenv
from fastapi import FastAPI

from kafka import KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger()

load_dotenv(verbose=True)

app = FastAPI()

@app.on_event('startup')
async def startup_event():
    """
    This startup function creates a topic and alter non topic config
    """
    
    client = KafkaAdminClient(bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
    topics = [
        NewTopic(name=os.environ.get('TOPICS_PEOPLE_BASIC_NAME'),
                 num_partitions=int(os.environ['TOPICS_PEOPLE_BASIC_PARTITIONS']),
                 replication_factor=int(os.environ['TOPICS_PEOPLE_BASIC_REPLICAS'])),
        NewTopic(name=f"{os.environ.get('TOPICS_PEOPLE_BASIC_NAME')}-short",
                 num_partitions=int(os.environ['TOPICS_PEOPLE_BASIC_PARTITIONS']),
                 replication_factor=int(os.environ['TOPICS_PEOPLE_BASIC_REPLICAS']),
                 topic_configs={
                     'retention.ms':'360000'
                 }
                 ),
    ]

    for topic in topics:
        try:
            client.create_topics([topic])
            logger.info('Topic Created')
        except TopicAlreadyExistsError as e:
            logger.warning("Topic Already Exists")

    cfg_resource_update = ConfigResource(
        ConfigResourceType.TOPIC,
        os.environ.get('TOPICS_PEOPLE_BASIC_NAME'),
        configs={
            'retention.ms': '360000'
        }
    )
    client.alter_configs([cfg_resource_update])

    client.close()


@app.get('/hello-world')
async def hello_world():
    return {'Hello World'}