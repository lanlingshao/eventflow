from eventflow.dispatcher.dispatcher import EventDispatcher, ConsumeMode, ConsumeResult


# 这个代码只是给出使用样例，实际使用时，需要根据业务场景进行调整
class StrictDispatcher(EventDispatcher):
    consume_mode = ConsumeMode.STRICT_ORDER

    async def _batch_handler_message(self, msgs):
        results = []
        for msg in msgs:
            try:
                await self.handle(msg)
                results.append(
                    ConsumeResult(
                        msg=msg,
                        success=True,
                    )
                )
            except Exception as exc:
                results.append(
                    ConsumeResult(
                        msg=msg,
                        success=False,
                        error=exc,
                    )
                )
        return results