from flask import Flask, send_from_directory
import logging

# Configure logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR) # Suppress standard Flask development server logs

app = Flask(__name__, static_folder='.')

@app.route('/')
def serve_index():
    """Serves the index.html file."""
    return send_from_directory('.', 'index.html')

if __name__ == '__main__':
    # You can change the port if needed
    port = 8080
    print(f"Flask server is running on http://127.0.0.1:{port}")
    print("This server will serve the index.html file for your Telegram Web App.")
    print("Use a tool like ngrok to expose this port to the internet via HTTPS.")
    app.run(host='0.0.0.0', port=port)
