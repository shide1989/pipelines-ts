// Minimal hello-world: the durable-sleep feature in isolation.
// signup → welcome email → 7-day sleep → check-in.

import { durable, sleep, workflow } from "pipelines";

const steps = durable({
  createUser: async (email: string) => {
    return { id: crypto.randomUUID(), email };
  },
  sendWelcome: async (email: string) => {
    return { sentTo: email, kind: "welcome" as const };
  },
  sendCheckIn: async (email: string) => {
    return { sentTo: email, kind: "check-in" as const };
  },
});

export const onboard = workflow("onboard", async (email: string) => {
  const user = await steps.createUser(email);
  await steps.sendWelcome(email);
  await sleep("7 seconds");
  await steps.sendCheckIn(email);
  return { userId: user.id };
});
