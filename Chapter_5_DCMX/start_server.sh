echo "running backend server"
cd dcmx/api
python server.py &
echo "running frontend server (no need for user_name and password)"
cd ../../dcmx-template
npm install
npm start &