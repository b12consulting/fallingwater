"""
Show how Pydantic AI message history grows across three runs.
"""
from pprint import pprint
from time import sleep

from pydantic_ai import Agent


agent = Agent()
conversation = None
conversation_uuid = None
messages = []
usage = None

for prompt in (
    "My name is Alice.",
    "Write 5 haiku about automation an technology",
    "Write 5 more",
    "Tell me a joke",
    "explain it",
    "What is the meaning of life ?",
    "What is my name",
):
    result = agent.run_sync(
        prompt,
        model="openai:gpt-6-luna",
        conversation_id=conversation_uuid,
        message_history=messages,
        usage=usage,
    )
    messages.extend(result.new_messages())
    conversation_uuid = result.conversation_id
    usage = result.usage

    # result = agent.run_sync(
    #     prompt,
    #     model="openai:gpt-6-luna",
    #     conversation=conversation
    # )
    # conversation = result.conversation

    print(f"\nAfter prompt: {prompt}")
    print(result.output)
    pprint(result.usage)
    sleep(0.5)
