import { AtpAgent, BlobRef } from '@atproto/api'

// YOUR bluesky handle
// Ex: user.bsky.social
const handle = `${process.env.FEEDGEN_HANDLE}`

// YOUR bluesky password, or preferably an App Password (found in your client settings)
// Ex: abcd-1234-efgh-5678
const password = `${process.env.FEEDGEN_PASSWORD}`


export async function getAgent() {
  const agent = new AtpAgent({ service: `${process.env.FEEDGEN_SUBSCRIPTION_ENDPOINT}` });
  await agent.login({ identifier: handle, password });
  return agent
}
