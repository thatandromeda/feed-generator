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
          console.log(`Skybrary candidate post found. Author: ${create.author}`)
          if (all_members.includes(create.author)) {
            console.log('Skybrary post found!')
            return true
          }
        }
        return false
      })
      .map((create) => {
        // map skybrarian posts to a db row
        console.log(
          `db row (except index timestamp): uri ${create.uri}, cid ${create.cid}, replyParent ${create.record?.reply?.parent.uri ?? null}, replyRoot ${create.record?.reply?.root.uri ?? null}`,
        )
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    // const timeStr = new Date(parseInt(indexedAt, 10)).toISOString()
    // builder = builder
    //   .where('post.indexedAt', '<', timeStr)
    //   .orWhere((qb) => qb.where('post.indexedAt', '=', timeStr))
    //   .where('post.cid', '<', cid)

    if (postsToDelete.length > 0) {
      const oneWeekAgoStr = new Date(
        new Date().getTime() - 7 * 24 * 60 * 60 * 1000,
      ).toISOString()
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .where('indexedAt', '<', oneWeekAgoStr)
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
    const res = await this.db
      .selectFrom('list_members')
      .selectAll('list_members')
      .execute()

    return res.map((member) => member.did)
  }

  // Runs as a periodic task in server.ts.
  // We don't want to hit rate limits by checking the list membership every time we
  // update posts on the feed, so we'll just do this every now and again.
  async updateMembers() {
    console.log('Updating members...')
    const lists: string[] = `${process.env.FEEDGEN_LISTS}`.split('|')
    const agent = await getAgent()
    const all_members: string[] = []
    const all_members_obj: { did: string }[] = []

    // Get members known to bluesky.
    while (lists.length > 0) {
      const list = lists.pop()
      let total_retrieved = 0
      let current_cursor: string | undefined = undefined

      // The original logic here was broken in cases where there were 0 mod 100 people on
      // the list, but my attempt to fix it truncated after 99 members. I'm just going to
      // assume there are fewer than 1000 skybrarians and hope for the best.
      while (total_retrieved <= 1000) {
        const list_members = await agent.api.app.bsky.graph.getList({
          list: `${list}`,
          limit: 100,
          cursor: current_cursor,
        })
        let current_retrieved = list_members.data.items.length

        if (current_retrieved === 0) {
          break
        }

        total_retrieved += current_retrieved
        current_cursor = list_members.data.cursor

        list_members.data.items.forEach((member) => {
          if (!all_members.includes(member.subject.did)) {
            all_members.push(member.subject.did)
          }
        })
      }
    }

    // Transform member DID list into a shape the db will understand.
    // (We didn't just start with this because itt was easier to use as a list in the
    // loop.)
    all_members.forEach((member) => {
      all_members_obj.push({ did: member })
    })

    // Drop all old list members from the db; add the new ones.
    await this.db.transaction().execute(async (trx) => {
      await trx.deleteFrom('list_members').executeTakeFirstOrThrow()
      await trx
        .replaceInto('list_members')
        .values(all_members_obj)
        .executeTakeFirstOrThrow()
    })
  }
}
