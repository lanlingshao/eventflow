import asyncio

from eventflow.dispatcher.dispatcher import EventDispatcher, ConsumeMode, ConsumeResult


# 这个代码只是给出使用样例，实际使用时，需要根据业务场景进行调整
class NormalDispatcher(EventDispatcher):
    consume_mode = ConsumeMode.NORMAL

    async def _batch_handler_message(self, msgs):
        tasks = [
            asyncio.create_task(self._consume(msg))
            for msg in msgs
        ]

        return await asyncio.gather(*tasks)

    async def _consume(self, msg):
        try:
            await self.handle(msg)
            return ConsumeResult(
                msg=msg,
                success=True,
            )
        except Exception as exc:
            return ConsumeResult(
                msg=msg,
                success=False,
                error=exc,
            )
