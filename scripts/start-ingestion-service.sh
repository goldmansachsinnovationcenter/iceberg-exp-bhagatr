
PID=$(pgrep -f "ingestion-service.jar" || echo "")
if [ ! -z "$PID" ]; then
    echo "Killing existing ingestion service process with PID: $PID"
    kill -9 $PID
fi

cd "$(dirname "$0")/../ingestion-service"

if [ ! -f "build/libs/ingestion-service.jar" ]; then
    echo "Building ingestion service..."
    ./gradlew clean bootJar
fi

echo "Starting ingestion service..."
java -jar build/libs/ingestion-service.jar "$@" > ../logs/ingestion-service.log 2>&1 &

echo "Ingestion service started with PID: $!"
