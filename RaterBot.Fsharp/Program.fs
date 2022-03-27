open System.Threading.Channels
open FSharp.Control
open Serilog
open Telegram.Bot
open System
open Microsoft.Data.Sqlite
open Microsoft.Extensions.DependencyInjection
open FluentMigrator.Runner
open Dapper
open System.IO
open System.Threading.Tasks
open Telegram.Bot.Types
open RaterBot.Database
open Telegram.Bot.Types.ReplyMarkups

let awaitIgnore (task: Task) = task |> Async.AwaitTask |> Async.Ignore

let mainChannel = Channel.CreateUnbounded<Async<unit>>()
let write x = mainChannel.Writer.TryWrite(x)

let logger = LoggerConfiguration().WriteTo.Console().CreateLogger();
//let botClient = TelegramBotClient(Environment.GetEnvironmentVariable("TELEGRAM_MEDIA_RATER_BOT_API"))
let botClient = TelegramBotClient("5284410585:AAEv0xKosSAR6OMB5IQLPNHWLcnOHooFsts")
let dbFolder = "db"
let dbPath = Path.Join(dbFolder, "sqlite.db")
let connectionString = SqliteConnectionStringBuilder(DataSource = dbPath).ConnectionString
let dbConnection = new SqliteConnection(connectionString)
let migrationConnectionString = SqliteConnectionStringBuilder(DataSource = dbPath, Mode = SqliteOpenMode.ReadWriteCreate).ConnectionString

let createServices() : IServiceProvider =
    let sc = ServiceCollection()
    let sc = sc.AddFluentMigratorCore()
    let sc = sc.ConfigureRunner(fun rb -> 
        rb.AddSQLite().WithGlobalConnectionString(migrationConnectionString).ScanIn(
            typeof<RaterBot.Database.Migrations.Init>.Assembly).For.Migrations()
        |> ignore)
    sc.BuildServiceProvider(false)

let migrateDatabase (sp: IServiceProvider) =
    let runner = sp.GetRequiredService<IMigrationRunner>()
    runner.MigrateUp()

//let handleNoInteraction (update: Update) (post: Post) = async {
//    let sql = "INSERT INTO Interaction (ChatId, UserId, MessageId, Reaction, PosterId) VALUES (@ChatId, @UserId, @MessageId, @Reaction, @PosterId);";
//    let sqlParams = {| ChatId = update.Message.Chat.Id; UserId = update.CallbackQuery.From.Id; update.Message.MessageId; Reaction = false; post.PosterId |}
//    do! dbConnection.ExecuteAsync(sql, sqlParams) |> awaitIgnore
//    //interactions.Add(Interaction(Reaction = newReaction))
//}

let handleCallback (update: Update) = async {
    let msg = update.CallbackQuery.Message
    let chatAndMessageIdParams = {| ChatId = msg.Chat.Id; MessageId = msg.MessageId |}
    let updateData = update.CallbackQuery.Data
    if updateData <> "-" && updateData <> "+" then
        logger.Warning("Invalid callback query data: {Data}", updateData)
        return ()
    else
        logger.Debug("Valid callback request")
        let sql = "SELECT * FROM Post WHERE ChatId = @ChatId AND MessageId = @MessageId;"
        let! post = dbConnection.QuerySingleOrDefaultAsync<Post>(sql, chatAndMessageIdParams) |> Async.AwaitTask
        let postOpt = Option.ofObj post
        match postOpt with
        | None ->
            logger.Error("Cannot find post in the database, ChatId = {ChatId}, MessageId = {MessageId}", msg.Chat.Id, msg.MessageId)
            try
                do! botClient.EditMessageReplyMarkupAsync(msg.Chat.Id, msg.MessageId, InlineKeyboardMarkup.Empty()) |> awaitIgnore
            with
                | e ->
                    logger.Warning(e, "Unable to set empty reply markup, trying to delete post")
                    do! botClient.DeleteMessageAsync(msg.Chat.Id, msg.MessageId) |> Async.AwaitTask
                    let sql = "DELETE FROM Interaction WHERE ChatId = @ChatId AND MessageId = @MessageId;"
                    do! dbConnection.QueryAsync<Interaction>(sql, chatAndMessageIdParams) |> awaitIgnore
        | Some post ->
            if post.PosterId = update.CallbackQuery.From.Id then
                do! botClient.AnswerCallbackQueryAsync(update.CallbackQuery.Id, "Нельзя голосовать за свои посты!") |> awaitIgnore
            else
                let sql = "SELECT * FROM Interaction WHERE ChatId = @ChatId AND Interaction.MessageId = @MessageId;"
                let! interactions =  dbConnection.QueryAsync<Interaction>(sql, chatAndMessageIdParams) |> Async.AwaitTask
                let interactions = ResizeArray<Interaction>(interactions)
                let interactionOpt = interactions |> Seq.tryFind (fun i -> i.UserId = update.CallbackQuery.From.Id)
                let newReaction = updateData = "+"
                match interactionOpt with
                | None ->
                    let sql = "INSERT INTO Interaction (ChatId, UserId, MessageId, Reaction, PosterId) VALUES (@ChatId, @UserId, @MessageId, @Reaction, @PosterId);";
                    let sqlParams = {| ChatId = msg.Chat.Id; UserId = update.CallbackQuery.From.Id; MessageId = msg.MessageId; Reaction = newReaction; PosterId = post.PosterId |}
                    do! dbConnection.ExecuteAsync(sql, sqlParams) |> awaitIgnore
                    interactions.Add(Interaction(Reaction = newReaction))
                | Some interaction ->
                    


    //if (post == null)
    //{
    //    _logger.Error("Cannot find post in the database, ChatId = {ChatId}, MessageId = {MessageId}", msg.Chat.Id, msg.MessageId);
    //    try
    //    {
    //        await botClient.EditMessageReplyMarkupAsync(msg.Chat.Id, msg.MessageId, InlineKeyboardMarkup.Empty());
    //    }
    //    catch (Telegram.Bot.Exceptions.ApiRequestException e)
    //    {
    //        _logger.Warning(e, "Unable to set empty reply markup, trying to delete post");
    //        await botClient.DeleteMessageAsync(msg.Chat.Id, msg.MessageId);
    //    }
    //    sql = $"SELECT * FROM {nameof(Interaction)} WHERE {nameof(Interaction.ChatId)} = @ChatId AND {nameof(Interaction.MessageId)} = @MessageId;";
    //    await connection.QueryAsync<Interaction>(sql, chatAndMessageIdParams);
    //    return;
    //}

    //if (post.PosterId == update.CallbackQuery.From.Id)
    //{
    //    await botClient.AnswerCallbackQueryAsync(update.CallbackQuery.Id, "Нельзя голосовать за свои посты!");
    //    return;
    //}

    //sql = $"SELECT * FROM {nameof(Interaction)} WHERE {nameof(Interaction.ChatId)} = @ChatId AND {nameof(Interaction.MessageId)} = @MessageId;";
    //var interactions = (await connection.QueryAsync<Interaction>(sql, chatAndMessageIdParams)).ToList();
    //var interaction = interactions.SingleOrDefault(i => i.UserId == update.CallbackQuery.From.Id);

    //var newReaction = updateData == "+";
    //if (interaction != null)
    //{
    //    if (newReaction == interaction.Reaction)
    //    {
    //        var reaction = newReaction ? "👍" : "👎";
    //        await botClient.AnswerCallbackQueryAsync(update.CallbackQuery.Id, $"Ты уже поставил {reaction} этому посту");
    //        _logger.Information("No need to update reaction");
    //        return;
    //    }
    //    sql = $"UPDATE {nameof(Interaction)} SET {nameof(Interaction.Reaction)} = @Reaction WHERE {nameof(Interaction.Id)} = @Id;";
    //    await connection.ExecuteAsync(sql, new { Reaction = newReaction, interaction.Id });
    //    interaction.Reaction = newReaction;
    //}
    //else
    //{
    //    sql = $"INSERT INTO {nameof(Interaction)} ({nameof(Interaction.ChatId)}, {nameof(Interaction.UserId)}, {nameof(Interaction.MessageId)}, {nameof(Interaction.Reaction)}, {nameof(Interaction.PosterId)}) VALUES (@ChatId, @UserId, @MessageId, @Reaction, @PosterId);";
    //    await connection.ExecuteAsync(sql, new { Reaction = newReaction, ChatId = msg.Chat.Id, UserId = update.CallbackQuery.From.Id, msg.MessageId, post.PosterId });
    //    interactions.Add(new Interaction { Reaction = newReaction });
    //}

    //var likes = interactions.Where(i => i.Reaction).Count();
    //var dislikes = interactions.Count - likes;

    //if (DateTime.UtcNow.AddMinutes(-5) > post.Timestamp && dislikes > 2 * likes + 3)
    //{
    //    _logger.Information("Deleting post. Dislikes = {Dislikes}, Likes = {Likes}", dislikes, likes);
    //    await botClient.DeleteMessageAsync(msg.Chat, msg.MessageId);
    //    sql = $"DELETE FROM {nameof(Post)} WHERE {nameof(Post.Id)} = @Id;";
    //    await _dbConnection.Value.ExecuteAsync(sql, new { post.Id });
    //    sql = $"DELETE FROM {nameof(Interaction)} WHERE {nameof(Interaction.ChatId)} = @ChatId AND {nameof(Interaction.MessageId)} = @MessageId;";
    //    var deletedRows = await _dbConnection.Value.ExecuteAsync(sql, chatAndMessageIdParams);
    //    _logger.Debug("Deleted {Count} rows from Interaction", deletedRows);
    //    await botClient.AnswerCallbackQueryAsync(update.CallbackQuery.Id, "Твой голос стал решающей каплей, этот пост удалён");
    //    return;
    //}

    //var plusText = likes > 0 ? $"{likes} 👍" : "👍";
    //var minusText = dislikes > 0 ? $"{dislikes} 👎" : "👎";

    //var ikm = new InlineKeyboardMarkup(new InlineKeyboardButton[]
    //{
    //    new InlineKeyboardButton(plusText){ CallbackData = "+" },
    //    new InlineKeyboardButton(minusText){ CallbackData = "-" }
    //});

    //try
    //{
    //    await botClient.EditMessageReplyMarkupAsync(msg.Chat, msg.MessageId, ikm);
    //    await botClient.AnswerCallbackQueryAsync(update.CallbackQuery.Id, "👌");
    //}
    //catch (Exception ex)
    //{
    //    _logger.Error(ex, "EditMessageReplyMarkupAsync");
    //}
    ()
}

let mainLoop = task {
    while true do
        let! coldTask = mainChannel.Reader.ReadAsync()
        do! coldTask
}

let handleUpdate botUser update =
    failwith "notimpl"
    
let getTelegramUpdateLoop () = task {
    let mutable offset = 0
    let! botUser = botClient.GetMeAsync()
    while true do
    try
        let! updates = botClient.GetUpdatesAsync(offset, 100, 1800)
        for u in updates do
            handleUpdate botUser u
        offset <- updates
            |> Seq.map (fun x -> x.Id)
            |> Seq.max
            |> (+) 1
    with
        | ex ->
            logger.Error(ex, "General update exception")
            do! Task.Delay(TimeSpan.FromSeconds(1))
}
    
[<EntryPoint>]
let main argv =
    if not <| Directory.Exists(dbFolder) then
        Directory.CreateDirectory(dbFolder) |> ignore

    SQLitePCL.Batteries.Init()

    let serviceProvider = createServices()
    (
        use scope = serviceProvider.CreateScope()
        migrateDatabase(scope.ServiceProvider)
    )

    dbConnection.Execute("PRAGMA synchronous = NORMAL;") |> ignore
    dbConnection.Execute("PRAGMA vacuum;") |> ignore
    dbConnection.Execute("PRAGMA temp_store = memory;") |> ignore
    
    let telegramUpdate = getTelegramUpdateLoop ()
    let ml = mainLoop |> Async.AwaitTask |> Async.RunSynchronously
    0