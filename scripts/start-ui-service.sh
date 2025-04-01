
PID=$(pgrep -f "ui-service.jar" || echo "")
if [ ! -z "$PID" ]; then
    echo "Killing existing UI service process with PID: $PID"
    kill -9 $PID
fi

cd "$(dirname "$0")/../ui-service"

if [ ! -f "build/libs/ui-service.jar" ]; then
    echo "Building UI service..."
    ./gradlew clean bootJar
fi

echo "Starting UI service..."
java -jar build/libs/ui-service.jar "$@" > ../logs/ui-service.log 2>&1 &

echo "UI service started with PID: $!"
