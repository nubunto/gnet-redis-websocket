import { parse } from "https://deno.land/std/flags/mod.ts";

const args = parse(Deno.args);

const ws = new WebSocket(args.server || 'wss://realtime-hub.fly.dev');

ws.addEventListener('open', () => {
  ws.send(JSON.stringify({
    action: 'message',
    channel: args.channel,
    message: {
      text: args.text,
      author: args.author,
      summary: args.summary,
      detail: args.detail
    },
  }));

  ws.close();
  Deno.exit(0);
});
