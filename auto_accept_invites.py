import argparse
import asyncio
import logging

from nio import AsyncClient, InviteMemberEvent, MatrixRoom

from config import Config


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("invite-acceptor")


class InviteAcceptor:
    def __init__(self, config: Config):
        self.config = config
        self.client = AsyncClient(config.MATRIX_HOMESERVER, config.MATRIX_USER_ID)
        self.client.access_token = config.MATRIX_ACCESS_TOKEN

    async def _join_room(self, room_id: str):
        try:
            await self.client.join(room_id)
            logger.info("Joined invite room: %s", room_id)
        except Exception as exc:
            logger.error("Failed joining %s: %s", room_id, exc)

    async def on_invite(self, room: MatrixRoom, event: InviteMemberEvent):
        if event.membership != "invite":
            return
        await self._join_room(room.room_id)

    async def accept_existing_invites(self):
        response = await self.client.sync(timeout=10000, full_state=True)
        invite_rooms = list(getattr(response.rooms, "invite", {}).keys())
        if not invite_rooms:
            logger.info("No pending invites found")
            return

        logger.info("Found %d pending invite(s)", len(invite_rooms))
        for room_id in invite_rooms:
            await self._join_room(room_id)

    async def run(self, watch: bool):
        self.client.add_event_callback(self.on_invite, InviteMemberEvent)
        await self.accept_existing_invites()

        if not watch:
            await self.client.close()
            return

        logger.info("Watching for new invites (Ctrl+C to stop)")
        try:
            await self.client.sync_forever(timeout=30000, full_state=False)
        finally:
            await self.client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-accept Matrix room invites")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep running and auto-accept future invites",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    config = Config()
    acceptor = InviteAcceptor(config)
    await acceptor.run(watch=args.watch)


if __name__ == "__main__":
    asyncio.run(main())
