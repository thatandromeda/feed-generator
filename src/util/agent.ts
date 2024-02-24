export async function getAgent() {
  const agent = new AtpAgent({ service: `${process.env.FEEDGEN_SUBSCRIPTION_ENDPOINT}` });
  await agent.login({ identifier: handle, password });
  return agent
}
