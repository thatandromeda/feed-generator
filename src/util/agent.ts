import { AtpAgent } from '@atproto/api'

// YOUR bluesky handle
// Ex: user.bsky.social
const handle = `${process.env.FEEDGEN_HANDLE}`

// YOUR bluesky password, or preferably an App Password (found in your client settings)
// Ex: abcd-1234-efgh-5678
const password = `${process.env.FEEDGEN_PASSWORD}`

export async function getAgent() {
  const agent = new AtpAgent({ service: 'https://bsky.social' })
  try {
    await agent.login({ identifier: handle, password })
  } catch (error) {
    // Wait a minute before trying again in case of rate limits.
    setTimeout(() => {
      console.log(`Could not log in: ${error}`)
    }, 60000)
  }
  console.log('💻 Logged in')
  return agent
}
