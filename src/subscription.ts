import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import { getAgent } from './util/agent'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    const all_members = await this.getMembers()

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only skybrarian posts
        if (
          create.record.text
            .toLowerCase()
            .includes(`${process.env.FEEDGEN_SYMBOL}`)
        ) {
          console.log(`We found something with the feedgen symbol: ${create}`)
          console.log(`Just in case, its author was: ${create.author}`)
          if (all_members.includes(create.author)) {
            return true
          }
        }
        return false
      })
      .map((create) => {
        // map skybrarian posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      console.log(`Time to create ${postsToCreate.length} post(s)`)
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }

  async getMembers() {
    const lists: string[] = `${process.env.FEEDGEN_LISTS}`.split('|')
    console.log(`📝 Lists found: ${lists}`)
    const agent = await getAgent()
    let all_members: string[] = []

    while (lists.length > 0) {
      const list = lists.pop()
      let total_retrieved = 100
      let current_cursor: string | undefined = undefined

      // total_retrieved will be smaller than 100 when we run off the end of the cursor...
      // unless we have a number of members evenly divisible by 100. Not sure how one was
      // intended to handle this.
      while (total_retrieved === 100) {
        const list_members = await agent.api.app.bsky.graph.getList({
          list: `${list}`,
          limit: 100,
          cursor: current_cursor,
        })
        total_retrieved = list_members.data.items.length
        current_cursor = list_members.data.cursor

        let escapeLoop = true
        list_members.data.items.forEach((member) => {
          if (!all_members.includes(member.subject.did)) {
            escapeLoop = false
            all_members.push(member.subject.did)
          }
        })

        // In the case that the number of list members is evenly divisible by 100, the
        // while loop will never terminate. We assume that, if we have added no new list
        // members to all_members in this pass, we must be repeating ourselves. Time to
        // flee the loop.
        if (escapeLoop) {
          break
        }
      }
    }

    return all_members
  }
}
