using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

public sealed partial class SqliteAgentStore
{
    public async Task<StoreCommandOutcome> StoreCommandAsync(CommandEnvelope command, DateTimeOffset receivedAtUtc, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var existingOutcome = await ResolveExistingCommandAsync(connection, transaction, command.CommandId, command.PayloadHash ?? string.Empty, receivedAtUtc, cancellationToken).ConfigureAwait(false);
        if (existingOutcome is not null)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return existingOutcome.Value;
        }

        var queueSequence = await NextQueueSequenceAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = """
            INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,next_attempt_at_utc,queue_sequence)
            VALUES($id,$type,$version,$priority,$ordering,$correlation,$payload,$hash,'queued',$created,$received,$notBefore,$expires,$received,$seq);
            """;
        Add(insert, "$id", command.CommandId.ToString("D")); Add(insert, "$type", command.CommandType); Add(insert, "$version", command.PayloadVersion);
        Add(insert, "$priority", command.Priority); Add(insert, "$ordering", command.OrderingKey); Add(insert, "$correlation", command.CorrelationId?.ToString("D"));
        Add(insert, "$payload", command.Payload.GetRawText()); Add(insert, "$hash", command.PayloadHash); Add(insert, "$created", command.CreatedAtUtc.ToUniversalTime().ToString("O"));
        Add(insert, "$received", receivedAtUtc.ToUniversalTime().ToString("O")); Add(insert, "$notBefore", command.NotBeforeUtc?.ToUniversalTime().ToString("O")); Add(insert, "$expires", command.ExpiresAtUtc?.ToUniversalTime().ToString("O")); Add(insert, "$seq", queueSequence);
        await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return StoreCommandOutcome.Stored;
    }

    public async Task<StoreCommandOutcome> AdmitCommandAsync(CommandEnvelope command, DateTimeOffset receivedAtUtc, ValidationResult validation, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var existingOutcome = await ResolveExistingCommandAsync(connection, transaction, command.CommandId, command.PayloadHash ?? string.Empty, receivedAtUtc, cancellationToken).ConfigureAwait(false);
        if (existingOutcome is not null)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return existingOutcome.Value;
        }

        var now = UtcNow();
        var payloadJson = SafePayloadJson(command.Payload);
        var payloadHash = command.PayloadHash ?? string.Empty;
        var commandType = command.CommandType ?? string.Empty;
        var queueSequence = await NextQueueSequenceAsync(connection, transaction, cancellationToken).ConfigureAwait(false);

        if (!validation.IsValid)
        {
            var resultJson = JsonSerializer.Serialize(new
            {
                commandId = command.CommandId,
                status = "dead_letter",
                completedAtUtc = DateTimeOffset.UtcNow,
                error = new { code = validation.ErrorCode, message = validation.Message, retryable = false },
                resultVersion = 1
            }, JsonOptions);
            await ExecuteAsync(connection, transaction,
                """
                INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,next_attempt_at_utc,finished_at_utc,result_status,result_json,last_error_code,last_error_message,queue_sequence)
                VALUES($id,$type,$version,$priority,$ordering,$correlation,$payload,$hash,'result_pending',$created,$received,$notBefore,$expires,NULL,$now,'dead_letter',$result,$code,$message,$seq);
                """,
                cancellationToken,
                ("$id", command.CommandId.ToString("D")), ("$type", commandType), ("$version", command.PayloadVersion),
                ("$priority", command.Priority), ("$ordering", command.OrderingKey), ("$correlation", command.CorrelationId?.ToString("D")),
                ("$payload", payloadJson), ("$hash", payloadHash), ("$created", command.CreatedAtUtc.ToUniversalTime().ToString("O")),
                ("$received", receivedAtUtc.ToUniversalTime().ToString("O")), ("$notBefore", command.NotBeforeUtc?.ToUniversalTime().ToString("O")),
                ("$expires", command.ExpiresAtUtc?.ToUniversalTime().ToString("O")), ("$now", now),
                ("$result", resultJson), ("$code", validation.ErrorCode), ("$message", validation.Message), ("$seq", queueSequence)).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction,
                "INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,next_attempt_at_utc,created_at_utc) VALUES($resultId,$id,$result,$hash,'pending',$now,$now);",
                cancellationToken,
                ("$resultId", Guid.NewGuid().ToString("D")), ("$id", command.CommandId.ToString("D")),
                ("$result", resultJson), ("$hash", PayloadHasher.ComputeBytes(Encoding.UTF8.GetBytes(resultJson))), ("$now", now)).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return StoreCommandOutcome.Rejected;
        }

        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = """
            INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,next_attempt_at_utc,queue_sequence)
            VALUES($id,$type,$version,$priority,$ordering,$correlation,$payload,$hash,'queued',$created,$received,$notBefore,$expires,$received,$seq);
            """;
        Add(insert, "$id", command.CommandId.ToString("D")); Add(insert, "$type", commandType); Add(insert, "$version", command.PayloadVersion);
        Add(insert, "$priority", command.Priority); Add(insert, "$ordering", command.OrderingKey); Add(insert, "$correlation", command.CorrelationId?.ToString("D"));
        Add(insert, "$payload", payloadJson); Add(insert, "$hash", payloadHash); Add(insert, "$created", command.CreatedAtUtc.ToUniversalTime().ToString("O"));
        Add(insert, "$received", receivedAtUtc.ToUniversalTime().ToString("O")); Add(insert, "$notBefore", command.NotBeforeUtc?.ToUniversalTime().ToString("O")); Add(insert, "$expires", command.ExpiresAtUtc?.ToUniversalTime().ToString("O")); Add(insert, "$seq", queueSequence);
        await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return StoreCommandOutcome.Stored;
    }

    private static async Task<StoreCommandOutcome?> ResolveExistingCommandAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        Guid commandId,
        string incomingPayloadHash,
        DateTimeOffset receivedAtUtc,
        CancellationToken cancellationToken)
    {
        string? existingPayloadHash;
        string? existingStatus;
        string? storedResultJson;
        await using (var existing = connection.CreateCommand())
        {
            existing.Transaction = transaction;
            existing.CommandText = "SELECT payload_hash,status,result_json FROM commands_inbox WHERE command_id=$id;";
            Add(existing, "$id", commandId.ToString("D"));
            await using var reader = await existing.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return null;
            existingPayloadHash = reader.GetString(0);
            existingStatus = reader.GetString(1);
            storedResultJson = reader.IsDBNull(2) ? null : reader.GetString(2);
        }

        if (!string.Equals(existingPayloadHash, incomingPayloadHash, StringComparison.Ordinal))
        {
            await RecordCommandPayloadConflictAsync(connection, transaction, commandId, existingPayloadHash, incomingPayloadHash, existingStatus, receivedAtUtc, cancellationToken).ConfigureAwait(false);
            return StoreCommandOutcome.PayloadConflict;
        }
        if (storedResultJson is not null)
        {
            string? existingResultHash;
            await using (var outbox = connection.CreateCommand())
            {
                outbox.Transaction = transaction;
                outbox.CommandText = "SELECT payload_hash FROM results_outbox WHERE command_id=$id;";
                Add(outbox, "$id", commandId.ToString("D"));
                existingResultHash = await outbox.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
            }

            var now = UtcNow();
            await ExecuteAsync(connection, transaction,
                """
                INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,next_attempt_at_utc,created_at_utc)
                VALUES($resultId,$id,$result,$resultHash,'pending',$now,$now)
                ON CONFLICT(command_id) DO UPDATE SET
                  status=CASE WHEN results_outbox.status='acknowledged' THEN 'pending' ELSE results_outbox.status END,
                  next_attempt_at_utc=CASE WHEN results_outbox.status='acknowledged' THEN excluded.next_attempt_at_utc ELSE results_outbox.next_attempt_at_utc END;
                """,
                cancellationToken,
                ("$resultId", Guid.NewGuid().ToString("D")), ("$id", commandId.ToString("D")), ("$result", storedResultJson),
                ("$resultHash", existingResultHash ?? PayloadHasher.ComputeBytes(Encoding.UTF8.GetBytes(storedResultJson))), ("$now", now)).ConfigureAwait(false);
        }
        return StoreCommandOutcome.Duplicate;
    }

    private static async Task RecordCommandPayloadConflictAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        Guid commandId,
        string originalDeclaredHash,
        string incomingDeclaredHash,
        string originalStatus,
        DateTimeOffset receivedAtUtc,
        CancellationToken cancellationToken)
    {
        var originalFingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(originalDeclaredHash)));
        var incomingFingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(incomingDeclaredHash)));
        var receivedAt = receivedAtUtc.ToUniversalTime().ToString("O");
        await ExecuteAsync(connection, transaction,
            """
            INSERT INTO command_payload_conflicts(event_id,command_id,event_code,severity,original_declared_hash_fingerprint,incoming_declared_hash_fingerprint,original_command_status,first_seen_at_utc,last_seen_at_utc,occurrence_count)
            VALUES($eventId,$id,'COMMAND_PAYLOAD_CONFLICT','critical',$originalFingerprint,$incomingFingerprint,$status,$receivedAt,$receivedAt,1)
            ON CONFLICT(command_id,original_declared_hash_fingerprint,incoming_declared_hash_fingerprint) DO UPDATE SET
              occurrence_count=command_payload_conflicts.occurrence_count+1,
              last_seen_at_utc=CASE WHEN excluded.last_seen_at_utc>command_payload_conflicts.last_seen_at_utc THEN excluded.last_seen_at_utc ELSE command_payload_conflicts.last_seen_at_utc END;
            """,
            cancellationToken,
            ("$eventId", Guid.NewGuid().ToString("D")), ("$id", commandId.ToString("D")),
            ("$originalFingerprint", originalFingerprint), ("$incomingFingerprint", incomingFingerprint),
            ("$status", originalStatus), ("$receivedAt", receivedAt)).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<CommandPayloadConflictEvent>> GetCommandPayloadConflictEventsAsync(int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var result = new List<CommandPayloadConflictEvent>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT event_id,command_id,event_code,severity,original_declared_hash_fingerprint,incoming_declared_hash_fingerprint,original_command_status,first_seen_at_utc,last_seen_at_utc,occurrence_count
            FROM command_payload_conflicts
            ORDER BY last_seen_at_utc DESC,command_id
            LIMIT $limit;
            """;
        Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add(new(
                Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetString(3),
                reader.GetString(4), reader.GetString(5), reader.GetString(6), ParseDate(reader.GetString(7)),
                ParseDate(reader.GetString(8)), reader.GetInt64(9)));
        }
        return result;
    }

    /// <summary>
    /// Allocates the stable order of retained queue rows from the persisted maximum inside the caller's
    /// serialized admission transaction. The value exceeds every retained row; retained rows are never
    /// renumbered. Values belonging to removed rows may be reused, including one after full cleanup.
    /// </summary>
    private static async Task<long> NextQueueSequenceAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT COALESCE(MAX(queue_sequence),0)+1 FROM commands_inbox;";
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static string SafePayloadJson(JsonElement payload) =>
        payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null ? "null" : payload.GetRawText();

    public Task<IReadOnlyList<StoredCommand>> GetReadyCommandsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken) =>
        GetReadyCommandsCoreAsync(limit, nowUtc, sentOnly: false, cancellationToken);

    public Task<IReadOnlyList<StoredCommand>> GetDueSentCommandsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken) =>
        GetReadyCommandsCoreAsync(limit, nowUtc, sentOnly: true, cancellationToken);

    private async Task<IReadOnlyList<StoredCommand>> GetReadyCommandsCoreAsync(int limit, DateTimeOffset nowUtc, bool sentOnly, CancellationToken cancellationToken)
    {
        var result = new List<StoredCommand>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Required queue semantics: one stable total order (priority DESC, received_at_utc,
        // queue_sequence) for BOTH selection and per-ordering-key head-of-line, so equal receive
        // times can never produce two heads of the same key. queue_sequence is the durable admission
        // tie-break (assigned at insert), which never reorders rows whose timestamps differ.
        // Ordering-release semantics preserved from baseline: an earlier predecessor blocks its
        // successor until it leaves the queue by ERP ACK (status -> 'completed') or a non-deliverable
        // terminal (cancelled/expired/dead_letter). A locally-completed predecessor in
        // 'result_pending' has NOT yet been acknowledged to ERP and therefore still blocks its
        // successor; only AcknowledgeResultAsync (status -> 'completed') releases it. This is
        // intentional: the ERP action for an ordering key must be acknowledged before the next one.
        // A07b: the sent-only variant applies the durable send-evidence predicate in SQL BEFORE
        // LIMIT (never a post-fetch filter, which would let a page of blocked fresh rows starve a
        // due sent row or spin on empty batches). The predicate is the same evidence set the
        // execution pass uses for live-row routing; the head-of-line guard applies identically, so
        // resolution never bypasses an active same-key predecessor.
        var sentEvidence = sentOnly
            ? "AND (c.status='unknown_result' OR c.attempt_count>0 OR c.post_attempt_count>0 OR c.first_sent_at_utc IS NOT NULL)"
            : string.Empty;
        command.CommandText = $"""
            SELECT c.command_id,c.command_type,c.payload_version,c.priority,c.ordering_key,c.correlation_id,c.payload_json,c.payload_hash,c.status,c.created_at_utc,c.received_at_utc,c.not_before_utc,c.expires_at_utc,c.attempt_count,c.next_attempt_at_utc,c.last_error_code,c.last_error_message,c.lookup_attempt_count AS lookup_attempt_count,c.post_attempt_count AS post_attempt_count,c.first_sent_at_utc AS first_sent_at_utc,c.queue_sequence AS queue_sequence
            FROM commands_inbox c
            WHERE c.status IN ('queued','retry_waiting','unknown_result')
              AND (c.not_before_utc IS NULL OR c.not_before_utc <= $now)
              AND (c.next_attempt_at_utc IS NULL OR c.next_attempt_at_utc <= $now)
              AND (c.ordering_key IS NULL OR NOT EXISTS (
                    SELECT 1 FROM commands_inbox p WHERE p.ordering_key=c.ordering_key
                    AND (p.received_at_utc < c.received_at_utc OR (p.received_at_utc = c.received_at_utc AND p.queue_sequence < c.queue_sequence))
                    AND p.status NOT IN ('completed','cancelled','expired','dead_letter')))
              {sentEvidence}
            ORDER BY c.priority DESC,c.received_at_utc,c.queue_sequence LIMIT $limit;
            """;
        Add(command, "$now", nowUtc.ToUniversalTime().ToString("O")); Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            using var document = JsonDocument.Parse(reader.GetString(6));
            var envelope = new CommandEnvelope(Guid.Parse(reader.GetString(0)), reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), NullableString(reader, 4),
                ParseGuid(NullableString(reader, 5)), ParseDate(reader.GetString(9)), ParseNullableDate(NullableString(reader, 11)), ParseNullableDate(NullableString(reader, 12)), null, reader.GetString(7), document.RootElement.Clone());
            result.Add(new StoredCommand(envelope, ParseCommandStatus(reader.GetString(8)), ParseDate(reader.GetString(10)), reader.GetInt32(13), ParseNullableDate(NullableString(reader, 14)), NullableString(reader, 15), NullableString(reader, 16), reader.GetInt32(17), reader.GetInt32(18), ParseNullableDate(NullableString(reader, 19)), reader.GetInt64(20)));
        }
        return result;
    }

    // Attempt identity is exact (command_attempts.attempt_id) and attempt numbering is a single
    // monotonic per-command audit sequence (MAX(attempt_no)+1). POST and lookup attempts share this
    // sequence and are told apart by attempt_kind only. This removes the old 1_000_000 offset and
    // the attempt_kind retro-rewrite — kind is written once at insert and never rewritten.
    //
    // Durable per-pass execution claim (A09): exactly one atomic claim per command generation is
    // acquired BEFORE any 1C call of a pass (status lookup or fresh POST). Two stale ready snapshots
    // or two concurrent claimants can therefore never both touch the network for the same command/key,
    // and owner-aware scheduling transitions can neither release nor mutate a claim they do not own.
    // The guarded UPDATE checks the due schedule (next_attempt_at_utc/not_before_utc), the current
    // state and the same-key head-of-line in the SAME transaction as the claim, and returns the live
    // row state so the pass re-decides on current data instead of on its (possibly stale) snapshot.
    // The legacy MarkExecutingAsync entry point uses a generated owner for its POST claim and retains
    // that claim after a successful call; normal callers use the owner-aware service lifecycle.
    public async Task<int> MarkExecutingAsync(Guid commandId, CancellationToken cancellationToken)
    {
        var owner = "legacy:" + Guid.NewGuid().ToString("D");
        var claim = await TryAcquireCommandExecutionClaimAsync(commandId, owner, DateTimeOffset.UtcNow, DateTimeOffset.MinValue, cancellationToken).ConfigureAwait(false);
        if (claim is null) throw new InvalidOperationException($"Command {commandId} cannot enter executing state.");
        try
        {
            var post = await ClaimPostAttemptAsync(commandId, owner, cancellationToken).ConfigureAwait(false);
            return post?.OperationalAttemptCount ?? throw new InvalidOperationException($"Command {commandId} cannot enter executing state.");
        }
        catch
        {
            await ReleaseCommandExecutionClaimAsync(commandId, owner, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Atomically acquires the durable per-pass execution claim for one command. The guarded UPDATE
    /// touches the row only when it is still awaiting a 1C outcome, the claim is free (or left stale
    /// by a dead process past the explicit <paramref name="staleBeforeUtc"/> boundary — production
    /// disables time takeover and clears crashed claims through <c>RecoverAsync</c> at startup), the
    /// due schedule (<c>not_before_utc</c>/<c>next_attempt_at_utc</c>) has arrived, and no earlier
    /// same-<c>ordering_key</c> predecessor still blocks head-of-line. All checks and the owner stamp
    /// commit in ONE transaction, and the live row state is RETURNED so the caller never executes on
    /// a stale snapshot. A NULL result means "do not call 1C and do not mutate anything".
    /// </summary>
    public async Task<ExecutionClaim?> TryAcquireCommandExecutionClaimAsync(Guid commandId, string ownerId, DateTimeOffset acquiredAtUtc, DateTimeOffset staleBeforeUtc, CancellationToken cancellationToken)
    {
        ValidateOwner(ownerId);
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = """
            UPDATE commands_inbox
            SET exec_claim_owner_id=$owner, exec_claim_acquired_at_utc=$now, row_version=row_version+1
            WHERE command_id=$id
              AND status IN ('queued','retry_waiting','unknown_result')
              AND (exec_claim_owner_id IS NULL OR exec_claim_acquired_at_utc IS NULL OR exec_claim_acquired_at_utc < $staleBefore)
              AND (not_before_utc IS NULL OR not_before_utc <= $now)
              AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc <= $now)
              AND (ordering_key IS NULL OR NOT EXISTS (
                    SELECT 1 FROM commands_inbox p WHERE p.ordering_key=commands_inbox.ordering_key
                    AND (p.received_at_utc < commands_inbox.received_at_utc OR (p.received_at_utc = commands_inbox.received_at_utc AND p.queue_sequence < commands_inbox.queue_sequence))
                    AND p.status NOT IN ('completed','cancelled','expired','dead_letter')))
            RETURNING status,attempt_count,lookup_attempt_count,post_attempt_count,first_sent_at_utc;
            """;
        Add(update, "$owner", ownerId); Add(update, "$now", acquiredAtUtc.ToUniversalTime().ToString("O"));
        Add(update, "$id", commandId.ToString("D")); Add(update, "$staleBefore", staleBeforeUtc.ToUniversalTime().ToString("O"));
        await using var reader = await update.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        // Claim failure => zero mutations in this transaction (rolls back) and the caller must not
        // perform any network call.
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return null;
        var currentStatus = ParseCommandStatus(reader.GetString(0));
        var currentAttemptCount = reader.GetInt32(1);
        var currentLookupAttemptCount = reader.GetInt32(2);
        var currentPostAttemptCount = reader.GetInt32(3);
        var currentFirstSentAtUtc = ParseNullableDate(NullableString(reader, 4));
        await reader.DisposeAsync().ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new ExecutionClaim(ownerId, acquiredAtUtc, staleBeforeUtc, currentStatus, currentAttemptCount, currentLookupAttemptCount, currentPostAttemptCount, currentFirstSentAtUtc);
    }

    /// <summary>
    /// Releases ONLY the claim owned by <paramref name="ownerId"/>. A newer claim generation (e.g. a
    /// takeover after this pass went stale) is never released or overwritten by an old snapshot.
    /// </summary>
    public async Task ReleaseCommandExecutionClaimAsync(Guid commandId, string ownerId, CancellationToken cancellationToken)
    {
        ValidateOwner(ownerId);
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var release = connection.CreateCommand();
        release.CommandText = "UPDATE commands_inbox SET exec_claim_owner_id=NULL,exec_claim_acquired_at_utc=NULL,row_version=row_version+1 WHERE command_id=$id AND exec_claim_owner_id=$owner;";
        Add(release, "$id", commandId.ToString("D")); Add(release, "$owner", ownerId);
        await release.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task<CommandAttemptId?> ClaimPostAttemptAsync(Guid commandId, CancellationToken cancellationToken) =>
        ClaimPostAttemptCoreAsync(commandId, claimOwner: null, cancellationToken: cancellationToken);

    public Task<CommandAttemptId?> ClaimPostAttemptAsync(Guid commandId, string claimOwner, CancellationToken cancellationToken)
    {
        ValidateOwner(claimOwner);
        return ClaimPostAttemptCoreAsync(commandId, claimOwner, cancellationToken);
    }

    private async Task<CommandAttemptId?> ClaimPostAttemptCoreAsync(Guid commandId, string? claimOwner, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var ownerPredicate = claimOwner is null ? "AND exec_claim_owner_id IS NULL" : "AND exec_claim_owner_id=$owner";
        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = $"""
            UPDATE commands_inbox
            SET status='executing',started_at_utc=$now,first_sent_at_utc=COALESCE(first_sent_at_utc,$now),attempt_count=attempt_count+1,post_attempt_count=post_attempt_count+1,row_version=row_version+1
            WHERE command_id=$id
              AND status IN ('queued','retry_waiting','unknown_result')
              {ownerPredicate}
            RETURNING attempt_count,payload_hash;
            """;
        Add(update, "$now", UtcNow()); Add(update, "$id", commandId.ToString("D"));
        if (claimOwner is not null) Add(update, "$owner", claimOwner);
        await using var reader = await update.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return null;
        var operationalAttempt = reader.GetInt32(0); var hash = reader.GetString(1);
        await reader.DisposeAsync().ConfigureAwait(false);
        var attempt = await InsertAttemptAsync(connection, transaction, commandId, hash, "post", cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new CommandAttemptId(attempt.AttemptId, attempt.AttemptNo, operationalAttempt);
    }

    public Task<CommandAttemptId?> ClaimLookupAttemptAsync(Guid commandId, string errorCode, string message, DateTimeOffset nextAttemptAtUtc, CancellationToken cancellationToken) =>
        ClaimLookupAttemptCoreAsync(commandId, claimOwner: null, errorCode, message, nextAttemptAtUtc, cancellationToken);

    public Task<CommandAttemptId?> ClaimLookupAttemptAsync(Guid commandId, string claimOwner, string errorCode, string message, DateTimeOffset nextAttemptAtUtc, CancellationToken cancellationToken)
    {
        ValidateOwner(claimOwner);
        return ClaimLookupAttemptCoreAsync(commandId, claimOwner, errorCode, message, nextAttemptAtUtc, cancellationToken);
    }

    private async Task<CommandAttemptId?> ClaimLookupAttemptCoreAsync(Guid commandId, string? claimOwner, string errorCode, string message, DateTimeOffset nextAttemptAtUtc, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var nextAttemptAt = nextAttemptAtUtc.ToUniversalTime().ToString("O");
        var ownerPredicate = claimOwner is null ? "AND exec_claim_owner_id IS NULL" : "AND exec_claim_owner_id=$owner";
        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = $"""
            UPDATE commands_inbox
            SET status='unknown_result',lookup_attempt_count=lookup_attempt_count+1,next_attempt_at_utc=$next,last_error_code=$code,last_error_message=$message,row_version=row_version+1
            WHERE command_id=$id
              AND status IN ('queued','retry_waiting','executing','unknown_result')
              {ownerPredicate}
            RETURNING lookup_attempt_count,payload_hash;
            """;
        Add(update, "$next", nextAttemptAt); Add(update, "$code", errorCode); Add(update, "$message", message); Add(update, "$id", commandId.ToString("D"));
        if (claimOwner is not null) Add(update, "$owner", claimOwner);
        await using var reader = await update.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return null;
        var lookupAttempt = reader.GetInt32(0); var hash = reader.GetString(1);
        await reader.DisposeAsync().ConfigureAwait(false);
        var attempt = await InsertAttemptAsync(connection, transaction, commandId, hash, "lookup", cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new CommandAttemptId(attempt.AttemptId, attempt.AttemptNo, lookupAttempt);
    }

    // Allocates the per-command monotonic audit sequence (MAX(attempt_no)+1) inside the caller's
    // transaction and inserts an UNFINISHED attempt row. Returns its exact identity.
    private static async Task<(Guid AttemptId, int AttemptNo)> InsertAttemptAsync(SqliteConnection connection, SqliteTransaction transaction, Guid commandId, string requestHash, string attemptKind, CancellationToken cancellationToken)
    {
        await using var next = connection.CreateCommand();
        next.Transaction = transaction;
        next.CommandText = "SELECT COALESCE(MAX(attempt_no),0)+1 FROM command_attempts WHERE command_id=$id;";
        Add(next, "$id", commandId.ToString("D"));
        var attemptNo = Convert.ToInt32(await next.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
        var attemptId = Guid.NewGuid();
        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = "INSERT INTO command_attempts(attempt_id,command_id,attempt_no,started_at_utc,request_hash,attempt_kind) VALUES($attemptId,$id,$attempt,$now,$hash,$kind);";
        Add(insert, "$attemptId", attemptId.ToString("D")); Add(insert, "$id", commandId.ToString("D")); Add(insert, "$attempt", attemptNo); Add(insert, "$now", UtcNow()); Add(insert, "$hash", requestHash); Add(insert, "$kind", attemptKind);
        await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return (attemptId, attemptNo);
    }

    // Closes EXACTLY ONE attempt by its identity (attempt_id), recording the audited post-network
    // outcome. Historical rows (older unknown/crashed attempts) are never touched or overwritten.
    public async Task CompleteAttemptAsync(Guid attemptId, CommandStatus outcome, string? errorCode, string? errorMessage, int? httpStatus, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE command_attempts SET finished_at_utc=$now,outcome=$outcome,error_code=$code,error_message=$message,http_status=$http WHERE attempt_id=$attemptId AND finished_at_utc IS NULL;";
        Add(command, "$now", UtcNow()); Add(command, "$outcome", ToDb(outcome)); Add(command, "$code", errorCode); Add(command, "$message", errorMessage); Add(command, "$http", httpStatus); Add(command, "$attemptId", attemptId.ToString("D"));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    // Scheduling transitions own the fresh-execution claim lifecycle: a pass that recorded an
    // unknown/retry outcome is over, so its matching claim is released in the same transaction and
    // the row is immediately claimable by the resolve-before-retry path (unknown_result stays
    // reclaimable per FR-CMD-012). Legacy transitions are unclaimed-only; owner-aware transitions
    // require the exact persisted generation. The outcome of one specific attempt is written on THAT
    // attempt row only (via CompleteAttemptAsync); these never touch command_attempts. A newer claim
    // generation is never released or overwritten.
    public Task<bool> MarkUnknownResultAsync(Guid commandId, Guid attemptId, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken) =>
        UpdateCommandAsync(commandId, "unknown_result", errorCode, message, nextAttemptAtUtc ?? default, claimOwner: null, cancellationToken);

    public Task<bool> MarkUnknownResultAsync(Guid commandId, Guid attemptId, string claimOwner, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken)
    {
        ValidateOwner(claimOwner);
        return UpdateCommandAsync(commandId, "unknown_result", errorCode, message, nextAttemptAtUtc ?? default, claimOwner, cancellationToken);
    }

    public Task<bool> MarkUnknownResultAsync(Guid commandId, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken) =>
        UpdateCommandAsync(commandId, "unknown_result", errorCode, message, nextAttemptAtUtc ?? default, claimOwner: null, cancellationToken);

    public Task<bool> MarkUnknownResultAsync(Guid commandId, string claimOwner, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken)
    {
        ValidateOwner(claimOwner);
        return UpdateCommandAsync(commandId, "unknown_result", errorCode, message, nextAttemptAtUtc ?? default, claimOwner, cancellationToken);
    }

    public Task<bool> ScheduleRetryAsync(Guid commandId, string errorCode, string message, DateTimeOffset retryAtUtc, CancellationToken cancellationToken) =>
        UpdateCommandAsync(commandId, "retry_waiting", errorCode, message, retryAtUtc, claimOwner: null, cancellationToken);

    public Task<bool> ScheduleRetryAsync(Guid commandId, string claimOwner, string errorCode, string message, DateTimeOffset retryAtUtc, CancellationToken cancellationToken)
    {
        ValidateOwner(claimOwner);
        return UpdateCommandAsync(commandId, "retry_waiting", errorCode, message, retryAtUtc, claimOwner, cancellationToken);
    }

    // Attempts resolved by a confirmed network response are closed by exact attempt_id via
    // CompleteAttemptAsync. This only seals the attempt that PRODUCED this terminal result
    // (resolvedAttemptId), never borrowing the outcome onto earlier unfinished (crashed) attempts.
    // The result_status column is the single source of truth for the operational dead-letter metric
    // (result_status='dead_letter'): the metric counts these rows from this moment and keeps counting
    // them after the ERP ACK (status becomes 'completed' while result_status is preserved), so a
    // command is counted exactly once.
    public Task<bool> CompleteLocallyAsync(Guid commandId, CommandStatus localStatus, string resultJson, string? externalRef, string? externalNumber, Guid? resolvedAttemptId, CancellationToken cancellationToken) =>
        CompleteLocallyCoreAsync(commandId, claimOwner: null, localStatus, resultJson, externalRef, externalNumber, resolvedAttemptId, cancellationToken);

    public Task<bool> CompleteLocallyAsync(Guid commandId, string claimOwner, CommandStatus localStatus, string resultJson, string? externalRef, string? externalNumber, Guid? resolvedAttemptId, CancellationToken cancellationToken)
    {
        ValidateOwner(claimOwner);
        return CompleteLocallyCoreAsync(commandId, claimOwner, localStatus, resultJson, externalRef, externalNumber, resolvedAttemptId, cancellationToken);
    }

    private async Task<bool> CompleteLocallyCoreAsync(Guid commandId, string? claimOwner, CommandStatus localStatus, string resultJson, string? externalRef, string? externalNumber, Guid? resolvedAttemptId, CancellationToken cancellationToken)
    {
        if (localStatus is not (CommandStatus.SucceededLocal or CommandStatus.BusinessFailedLocal or CommandStatus.Expired or CommandStatus.Cancelled or CommandStatus.DeadLetter)) throw new ArgumentOutOfRangeException(nameof(localStatus));
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var ownerPredicate = claimOwner is null ? "AND exec_claim_owner_id IS NULL" : "AND exec_claim_owner_id=$owner";
        var cancellationPredicate = localStatus is CommandStatus.Cancelled
            ? "AND status IN ('queued','retry_waiting') AND attempt_count=0 AND post_attempt_count=0 AND first_sent_at_utc IS NULL"
            : string.Empty;
        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = $"""
            UPDATE commands_inbox
            SET status='result_pending',result_status=$resultStatus,result_json=$result,external_ref=$ref,external_number=$number,finished_at_utc=$now,next_attempt_at_utc=NULL,exec_claim_owner_id=NULL,exec_claim_acquired_at_utc=NULL,row_version=row_version+1
            WHERE command_id=$id
              AND status IN ('queued','retry_waiting','executing','unknown_result')
              {ownerPredicate}
              {cancellationPredicate};
            """;
        Add(update, "$resultStatus", ToDb(localStatus)); Add(update, "$result", resultJson); Add(update, "$ref", externalRef); Add(update, "$number", externalNumber); Add(update, "$now", now); Add(update, "$id", commandId.ToString("D"));
        if (claimOwner is not null) Add(update, "$owner", claimOwner);
        var changed = await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (changed == 0)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }
        await ExecuteAsync(connection, transaction,
            "INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,next_attempt_at_utc,created_at_utc) VALUES($resultId,$id,$result,$hash,'pending',$now,$now) ON CONFLICT(command_id) DO UPDATE SET payload_json=excluded.payload_json,payload_hash=excluded.payload_hash,status=CASE WHEN results_outbox.status='acknowledged' THEN results_outbox.status ELSE 'pending' END,next_attempt_at_utc=excluded.next_attempt_at_utc;",
            cancellationToken, ("$resultId", Guid.NewGuid().ToString("D")), ("$id", commandId.ToString("D")), ("$result", resultJson), ("$hash", PayloadHasher.ComputeBytes(Encoding.UTF8.GetBytes(resultJson))), ("$now", now)).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE command_attempts SET finished_at_utc=$now,outcome=$outcome WHERE attempt_id=$attemptId AND command_id=$id AND finished_at_utc IS NULL;",
            cancellationToken, ("$now", now), ("$outcome", ToDb(localStatus)), ("$attemptId", resolvedAttemptId?.ToString("D")), ("$id", commandId.ToString("D"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<IReadOnlyList<PendingResult>> GetPendingResultsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var results = new List<PendingResult>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT command_id,payload_json,attempt_count FROM results_outbox WHERE status IN ('pending','retry_waiting') AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc <= $now) ORDER BY created_at_utc LIMIT $limit;";
        Add(command, "$now", nowUtc.ToUniversalTime().ToString("O")); Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) results.Add(new PendingResult(Guid.Parse(reader.GetString(0)), reader.GetString(1), reader.GetInt32(2)));
        return results;
    }

    public async Task<bool> MarkResultRetryAsync(Guid commandId, string errorMessage, DateTimeOffset retryAtUtc, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE results_outbox
            SET status='retry_waiting',attempt_count=attempt_count+1,next_attempt_at_utc=$retry,last_error=$error
            WHERE command_id=$id
              AND status IN ('pending','retry_waiting','sending')
              AND EXISTS (
                    SELECT 1 FROM commands_inbox c
                    WHERE c.command_id=results_outbox.command_id
                      AND c.status IN ('result_pending','completed')
                      AND c.result_status IS NOT NULL
                      AND c.result_json IS NOT NULL);
            """;
        Add(command, "$retry", retryAtUtc.ToUniversalTime().ToString("O"));
        Add(command, "$error", errorMessage);
        Add(command, "$id", commandId.ToString("D"));
        var changed = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return changed == 1;
    }

    public async Task<bool> AcknowledgeResultAsync(Guid commandId, DateTimeOffset acknowledgedAtUtc, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var at = acknowledgedAtUtc.ToUniversalTime().ToString("O");
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE commands_inbox
            SET status='completed',erp_acknowledged_at_utc=$at,row_version=row_version+1
            WHERE command_id=$id
              AND status IN ('result_pending','completed')
              AND result_status IS NOT NULL
              AND result_json IS NOT NULL
              AND EXISTS (
                    SELECT 1 FROM results_outbox o
                    WHERE o.command_id=commands_inbox.command_id
                      AND o.status IN ('pending','retry_waiting','sending'));
            """;
        Add(command, "$at", at);
        Add(command, "$id", commandId.ToString("D"));
        var commandChanged = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (commandChanged == 0)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }

        await using var outbox = connection.CreateCommand();
        outbox.Transaction = transaction;
        outbox.CommandText = """
            UPDATE results_outbox
            SET status='acknowledged',acknowledged_at_utc=$at,sent_at_utc=$at
            WHERE command_id=$id
              AND status IN ('pending','retry_waiting','sending')
              AND EXISTS (
                    SELECT 1 FROM commands_inbox c
                    WHERE c.command_id=results_outbox.command_id
                      AND c.status='completed'
                      AND c.result_status IS NOT NULL
                      AND c.result_json IS NOT NULL);
            """;
        Add(outbox, "$at", at);
        Add(outbox, "$id", commandId.ToString("D"));
        var outboxChanged = await outbox.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (outboxChanged != 1)
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            return false;
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static void ValidateOwner(string? claimOwner)
    {
        ArgumentNullException.ThrowIfNull(claimOwner);
        if (string.IsNullOrWhiteSpace(claimOwner)) throw new ArgumentException("Claim owner must not be blank.", nameof(claimOwner));
    }

    private async Task<bool> UpdateCommandAsync(Guid commandId, string status, string errorCode, string message, DateTimeOffset retryAtUtc, string? claimOwner, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var retry = (retryAtUtc == default ? DateTimeOffset.UtcNow.AddSeconds(2) : retryAtUtc).ToUniversalTime().ToString("O");
        var ownerPredicate = claimOwner is null ? "AND exec_claim_owner_id IS NULL" : "AND exec_claim_owner_id=$owner";
        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = $"""
            UPDATE commands_inbox
            SET status=$status,last_error_code=$code,last_error_message=$message,next_attempt_at_utc=$retry,exec_claim_owner_id=NULL,exec_claim_acquired_at_utc=NULL,row_version=row_version+1
            WHERE command_id=$id
              AND status IN ('queued','retry_waiting','executing','unknown_result')
              {ownerPredicate};
            """;
        Add(update, "$status", status);
        Add(update, "$code", errorCode);
        Add(update, "$message", message);
        Add(update, "$retry", retry);
        Add(update, "$id", commandId.ToString("D"));
        if (claimOwner is not null) Add(update, "$owner", claimOwner);
        var changed = await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return changed != 0;
    }
}
