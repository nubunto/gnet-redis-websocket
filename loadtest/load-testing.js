import { WebSocket} from 'k6/x/websockets';
import { sleep } from 'k6';
import { randomString, randomIntBetween } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import { setTimeout, clearTimeout, setInterval, clearInterval } from "k6/x/events"

export const options = {
  stages: [
    { duration: '20s', target: 1 },
    { duration: '40s', target: 5 },
    { duration: '10s', target: 0 },
  ]
};

export default function () {
  startWSWorker(randomString(10));
  startWSWorker(randomString(10));
}

function startWSWorker(id) {
  let url = 'wss://realtime-hub.fly.dev';
  let ws = new WebSocket(url);
  let intervalRef;
  let channel = randomString(5);

  let timeoutId = setTimeout(() => {
    clearInterval(intervalRef);
    ws.close(1006, 'done');
  }, 60000)

  ws.addEventListener('open', () => {
    ws.send(j({action: 'join', channel: channel}))
    ws.addEventListener('message', e => {
      let message = JSON.parse(e.data)
      if (message.success) {
        intervalRef = setInterval(() => {
          ws.send(j({channel: channel, message: `${id} message!`}))
        }, 1000)
      }
    })
  })
}

function j(obj) {
  return JSON.stringify(obj)
}
