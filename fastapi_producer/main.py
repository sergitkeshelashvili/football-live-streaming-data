from fastapi import FastAPI
from datetime import datetime, timezone
import uuid, json, random, asyncio
from kafka import KafkaProducer
import signal

# ---------------------------------------
# App initialization
# ---------------------------------------
app = FastAPI(title="Football Event API")

# ---------------------------------------
# Kafka configuration
# ---------------------------------------
TOPIC_NAME = "football_events"
BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:19092', 'localhost:29092']

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5
)

# ---------------------------------------
# Teams & players
# ---------------------------------------
teams = {
    "TeamA": ["Player1","Player2","Player3"],
    "TeamB": ["Player4","Player5","Player6"]
}

event_types = ["goal","shot_on_target","foul","yellow_card","red_card","substitution"]

# ---------------------------------------
# Generate a single event
# ---------------------------------------
def generate_random_event(match_id: str) -> dict:
    team = random.choice(list(teams.keys()))
    player = random.choice(teams[team])
    event_type = random.choice(event_types)
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "match_id": match_id,
        "team": team,
        "player": player,
        "event_type": event_type,
        "timestamp": timestamp
    }

# ---------------------------------------
# Background simulation tasks
# ---------------------------------------
simulation_tasks = []

async def simulate_match(match_id: str, events_per_sec: int = 50):
    interval = 1 / events_per_sec
    while True:
        event = generate_random_event(match_id)
        # Fire-and-forget Kafka send (thread-safe)
        producer.send(TOPIC_NAME, event)
        await asyncio.sleep(interval)

# ---------------------------------------
# API endpoint: start simulation
# ---------------------------------------
@app.post("/start_simulation")
async def start_simulation(num_matches: int = 10, events_per_sec: int = 50):
    """
    Start background simulation for multiple football matches.
    """
    global simulation_tasks
    for i in range(num_matches):
        match_id = f"match_{i+1}"
        task = asyncio.create_task(simulate_match(match_id, events_per_sec))
        simulation_tasks.append(task)

    return {
        "status": "simulation_started",
        "matches": num_matches,
        "events_per_second_per_match": events_per_sec
    }

# ---------------------------------------
# API endpoint: stop simulation
# ---------------------------------------
@app.post("/stop_simulation")
async def stop_simulation():
    global simulation_tasks
    for task in simulation_tasks:
        task.cancel()
    simulation_tasks = []
    return {"status": "simulation_stopped"}

# ---------------------------------------
# API endpoint: generate single event
# ---------------------------------------
@app.post("/event")
async def create_event(match_id: str):
    event = generate_random_event(match_id)
    producer.send(TOPIC_NAME, event)
    return {"status": "sent", "event": event}

# ---------------------------------------
# Shutdown: flush Kafka producer
# ---------------------------------------
def shutdown_handler(*args):
    print("Flushing Kafka producer...")
    producer.flush()
    producer.close()
    print("Producer closed. Exiting.")
    exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ---------------------------------------
# Run server
# ---------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Disable reload in production
        workers=2
    )
