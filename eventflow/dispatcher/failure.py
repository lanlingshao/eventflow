from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum

from eventflow.dispatcher.consumer import ConsumerMessage


class FailureAction(StrEnum):
    ACK = "ack"
    RETRY = "retry"
    DLQ = "dlq"


@dataclass
class FailureContext:
    msg: ConsumerMessage
    error: Exception
    retry_count: int


@dataclass
class FailureDecision:
    action: FailureAction
    retry_count: int | None = None


class FailureHandlingStrategy(ABC):

    @property
    @abstractmethod
    def actions(self) -> set[FailureAction]:
        ...

    @abstractmethod
    async def decide(self, context: FailureContext) -> FailureDecision:
        ...


class NoopFailureStrategy(FailureHandlingStrategy):

    @property
    def actions(self) -> set[FailureAction]:
        return {FailureAction.ACK}

    async def decide(
        self,
        context: FailureContext,
    ) -> FailureDecision:
        return FailureDecision(
            action=FailureAction.ACK,
        )


class MaxRetryStrategy(FailureHandlingStrategy):
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries

    @property
    def actions(self) -> set[FailureAction]:
        return {
            FailureAction.RETRY,
            FailureAction.DLQ,
        }

    async def decide(self, context: FailureContext) -> FailureDecision:
        if context.retry_count < self.max_retries:
            return FailureDecision(
                action=FailureAction.RETRY,
                retry_count=context.retry_count + 1,
            )
        return FailureDecision(
            action=FailureAction.DLQ,
            retry_count=context.retry_count + 1,
        )
