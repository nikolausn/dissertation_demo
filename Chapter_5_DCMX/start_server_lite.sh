echo "running backend server"
cd dcmx/api
python server.py &
echo "running frontend server lite version (for local codespace via vscode)"
cd ../../dcmx-ts
npm install --force
npm start &