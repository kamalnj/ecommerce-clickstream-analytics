from confluent_kafka import Producer

# 1. Se connecter au broker (adresse vue depuis ton PC)
producer = Producer({"bootstrap.servers": "localhost:9092"})

# 2. Envoyer un message dans le topic
producer.produce("evenements_web", key="test", value='{"message": "bonjour Kafka"}')

# 3. Attendre que le message soit vraiment parti
producer.flush()
print("Message envoyé")
