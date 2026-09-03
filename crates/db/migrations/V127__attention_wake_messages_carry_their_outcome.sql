-- Wake prompts admitted by the wake-turn consumer are system messages. Their
-- `outcome` now names them (`attention_wake`) so the chat timeline can
-- collapse the work order to its summary line instead of showing the whole
-- prompt. Messages are immutable; the update guard is lifted only for this
-- backfill of prompts admitted before the outcome existed.
DROP TRIGGER IF EXISTS agent_chat_message_immutable_update;

UPDATE agent_chat_message
   SET outcome = 'attention_wake'
 WHERE author_type = 'system'
   AND outcome IS NULL
   AND content LIKE '### Attention wake:%';

CREATE TRIGGER agent_chat_message_immutable_update
BEFORE UPDATE ON agent_chat_message
BEGIN
    SELECT RAISE(ABORT, 'Agent Chat messages are immutable');
END;
