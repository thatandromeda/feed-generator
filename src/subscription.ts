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

    const all_members = await getMembers()

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only skybrarian posts
        if create.record.text.toLowerCase().includes(`${process.env.FEEDGEN_SYMBOL}`) {
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
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
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
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }

  async function getMembers() {
    const lists:string[] = `${process.env.FEEDGEN_LISTS}`.split("|")
    const agent = await getAgent()

    while (lists.length > 0) {
      const list = lists.pop()
      let total_retrieved = 1
      let current_cursor:string|undefined = undefined
      let all_members = []

      while (total_retrieved > 0) {
        const list_members = await agent.api.app.bsky.graph.getList({list:`${list}`,limit:100,cursor:current_cursor})
        total_retrieved = list_members.data.items.length
        current_cursor = list_members.data.cursor

        list_members.data.items.forEach((member) => {
            if (!all_members.includes(member.subject.did)) all_members.push(member.subject.did)
        })
      }
    }

    return all_members
  }
}
