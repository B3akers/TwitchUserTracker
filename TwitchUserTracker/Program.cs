using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TwitchUserTracker
{
    public static class TrackerConfiguration
    {
        public static readonly string MongoConnectionUrl = Secrets.MongoConnectionUrl;
        public const long LastLiveCheckTime = 3600; // seconds
        public const int MinimumViewersGlobal = 1000;
        public const int MinimumViewersPolish = 100;
        public const int UpdateLiveChannelsInterval = 20; //minutes
        public const int MaxUpdaterTasks = 15;
    }

    public class TwitchTrackedChannel
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string ID;

        public string BroadcasterId;
        public string BroadcasterName;
        public string BroadcasterLogin;
        public long LastCheckLiveTime;
    }

    public class TwitchTrackedUser
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string ID;

        public string DisplayName;
        public string BroadcasterId;
        public long LastVisited;
        public long Count;
    }

    public class BroadcasterInfo
    {
        public string BroadcasterId { get; set; }
        public string BroadcasterName { get; set; }
        public string BroadcasterLogin { get; set; }
        public string Title { get; set; }
        public List<string> Tags { get; set; }
        public override int GetHashCode()
        {
            return BroadcasterId.GetHashCode();
        }
        public override bool Equals(object obj)
        {
            return obj is BroadcasterInfo && Equals((BroadcasterInfo)obj);
        }

        public bool Equals(BroadcasterInfo p)
        {
            return BroadcasterId == p.BroadcasterId;
        }
    }

    public class LiveChannelInfo
    {
        public string Cursor { get; set; }
        public BroadcasterInfo Broadcaster { get; set; }
        public int ViewersCount { get; set; }
    }

    public class StreamTags
    {
        public static readonly string PolandTagId = "\"340f150f-3371-4055-9350-5c559ea913ca\"";
        public static readonly string PolishTagId = "\"f9d04efa-6e25-49bf-bf0a-da3e2addaf1b\"";
    }

    class Program
    {
        static Task Main(string[] args) => new Program().MainLoop();

        private readonly HttpClient _httpClient = new HttpClient();

        private IMongoDatabase mongoDatabase;
        private IMongoCollection<TwitchTrackedChannel> trackedChannelsCollection;
        private IMongoCollection<TwitchTrackedUser> trackedUsersCollection;

        private CancellationTokenSource cancellationToken;

        private bool updateChatterTaskStarted = false;

        private List<Task> currentTasks = new List<Task>();

        private ConcurrentDictionary<string, long> _lastChannelsUpdates = new ConcurrentDictionary<string, long>();
        private HashSet<string> polishWordList = new HashSet<string>();

        private char[] removePolishCharacters(char[] word)
        {
            for (var i = 0; i < word.Length; i++)
            {
                switch (word[i])
                {
                    case 'ą':
                        word[i] = 'a';
                        break;
                    case 'ć':
                        word[i] = 'c';
                        break;
                    case 'ę':
                        word[i] = 'e';
                        break;
                    case 'ł':
                        word[i] = 'l';
                        break;
                    case 'ń':
                        word[i] = 'n';
                        break;
                    case 'ó':
                        word[i] = 'o';
                        break;
                    case 'ś':
                        word[i] = 's';
                        break;
                    case 'ż':
                    case 'ź':
                        word[i] = 'z';
                        break;
                }
            }

            return word;
        }

        private async Task<List<LiveChannelInfo>> GetPopularLiveChannels(string cursor = null, List<string> tags = null)
        {
            var response = await _httpClient.PostAsync("https://gql.twitch.tv/gql", new StringContent("[{\"operationName\":\"BrowsePage_Popular\",\"variables\":{" + (string.IsNullOrEmpty(cursor) ? string.Empty : $"\"cursor\":\"{cursor}\",") + "\"limit\":30,\"platformType\":\"all\",\"options\":{\"recommendationsContext\":{\"platform\":\"web\"},\"sort\":\"VIEWER_COUNT\",\"tags\":[" + (tags == null ? string.Empty : string.Join(',', tags)) + "]},\"sortTypeIsRecency\":false,\"freeformTagsEnabled\":false},\"extensions\":{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"267d2d2a64e0a0d6206c039ea9948d14a9b300a927d52b2efc52d2486ff0ec65\"}}}]", System.Text.Encoding.UTF8, "text/plain"));

            response.EnsureSuccessStatusCode();

            var responseBody = await response.Content.ReadAsStringAsync();
            var jsonArray = JArray.Parse(responseBody);

            List<LiveChannelInfo> channelList = new List<LiveChannelInfo>();

            if (jsonArray.Count == 1)
            {
                var edgesList = jsonArray[0]["data"]["streams"]["edges"] as JArray;

                foreach (var edge in edgesList)
                {
                    try
                    {
                        List<string> channelTags = new List<string>();

                        var channelTagsArray = edge["node"]["tags"] as JArray;
                        if (channelTagsArray != null)
                            foreach (var tag in channelTagsArray)
                                channelTags.Add("\"" + (string)tag["id"] + "\"");

                        channelList.Add(new LiveChannelInfo()
                        {
                            Cursor = (string)edge["cursor"],
                            Broadcaster = new BroadcasterInfo()
                            {
                                BroadcasterId = (string)edge["node"]["broadcaster"]["id"],
                                BroadcasterName = (string)edge["node"]["broadcaster"]["displayName"],
                                BroadcasterLogin = (string)edge["node"]["broadcaster"]["login"],
                                Title = (string)edge["node"]["title"],
                                Tags = channelTags
                            },
                            ViewersCount = (int)edge["node"]["viewersCount"]
                        });
                    }
                    catch (Exception es) { Console.WriteLine(es); }
                }
            }

            return channelList;
        }

        private async Task LoopForLiveChannelToTracks(Action<LiveChannelInfo> predicate, int minimumViewers)
        {
            List<LiveChannelInfo> channelList;
            string cursor = null;

            do
            {
                channelList = await GetPopularLiveChannels(cursor);

                var shouldBreak = cancellationToken.Token.IsCancellationRequested;

                foreach (var channel in channelList)
                {
                    if (channel.ViewersCount < minimumViewers)
                    {
                        shouldBreak = true;
                        break;
                    }

                    predicate(channel);
                }

                if (shouldBreak)
                    break;

                if (channelList.Count > 0)
                    cursor = channelList[channelList.Count - 1].Cursor;

            } while (channelList.Count > 0 && !string.IsNullOrEmpty(cursor));
        }

        private async Task GetLiveChannelToTracks()
        {
            HashSet<BroadcasterInfo> broadcasterInfos = new HashSet<BroadcasterInfo>();

            await LoopForLiveChannelToTracks((LiveChannelInfo channel) =>
            {
                if (channel.ViewersCount >= TrackerConfiguration.MinimumViewersGlobal || channel.Broadcaster.Tags.Contains(StreamTags.PolishTagId) || channel.Broadcaster.Tags.Contains(StreamTags.PolandTagId))
                {
                    broadcasterInfos.Add(channel.Broadcaster);
                    return;
                }

                //Check title for polish, some streamers doesnt set polish tag

                var polishWords = 0;
                var titleWords = channel.Broadcaster.Title.Split(' ').Where(x => x.Length > 2 && !x.StartsWith("!")).Select(x => new string(removePolishCharacters(x.Replace("!", "").Replace(".", "").ToLower().Trim().ToCharArray()))).ToArray();

                if (titleWords.Length < 2)
                    return;

                foreach (var word in titleWords)
                    if (polishWordList.Contains(word))
                        polishWords++;

                float percent = (float)polishWords / titleWords.Length;

                if (percent > 0.3f)
                    broadcasterInfos.Add(channel.Broadcaster);

            }, Math.Min(TrackerConfiguration.MinimumViewersPolish, TrackerConfiguration.MinimumViewersGlobal));

            var currentTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            foreach (var broadcaster in broadcasterInfos)
            {
                await trackedChannelsCollection.UpdateOneAsync(x => x.BroadcasterId == broadcaster.BroadcasterId, new BsonDocument("$set", new BsonDocument { { "BroadcasterName", broadcaster.BroadcasterName }, { "BroadcasterLogin", broadcaster.BroadcasterLogin }, { "LastCheckLiveTime", currentTime } }), new UpdateOptions()
                {
                    IsUpsert = true
                });
            }
        }

        private async Task UpdateLiveChannelToTracksLoop()
        {
            var period = TimeSpan.FromMinutes(5);
            while (!cancellationToken.Token.IsCancellationRequested)
            {
                Console.WriteLine("UpdateLiveChannelToTracksLoop start");
                try
                {
                    await GetLiveChannelToTracks();

                    {
                        List<string> removeNonActiveChannels = new List<string>();
                        var currentUnixTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        foreach (var channel in _lastChannelsUpdates)
                        {
                            if (currentUnixTimestamp - channel.Value > (period.TotalSeconds * 2))
                                removeNonActiveChannels.Add(channel.Key);
                        }

                        if (removeNonActiveChannels.Count > 0)
                            Console.WriteLine($"Removing {removeNonActiveChannels.Count} channels!");

                        foreach (var key in removeNonActiveChannels)
                            _lastChannelsUpdates.Remove(key, out _);
                    }

                }
                catch (Exception es) { Console.WriteLine(es); }
                Console.WriteLine("UpdateLiveChannelToTracksLoop end");

                if (!updateChatterTaskStarted)
                {
                    currentTasks.Add(Task.Run(() => UpdateChannelsChattersLoop(period)));
                    updateChatterTaskStarted = true;
                }

                try
                {
                    await Task.Delay(1000 * 60 * TrackerConfiguration.UpdateLiveChannelsInterval, cancellationToken.Token);
                }
                catch (OperationCanceledException) { }
            }
        }

        private async Task UpdateChannelChatters(TwitchTrackedChannel channel)
        {
            try
            {
                if (string.IsNullOrEmpty(channel.BroadcasterLogin))
                    channel.BroadcasterLogin = channel.BroadcasterName.ToLower();

                var chattersStringData = await _httpClient.GetStringAsync($"https://tmi.twitch.tv/group/user/{channel.BroadcasterLogin}/chatters");
                var chattersData = JObject.Parse(chattersStringData);
                var chatters = chattersData["chatters"];
                var currentTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                _lastChannelsUpdates.AddOrUpdate(channel.BroadcasterId, currentTime, (key, oldValue) => currentTime);

                var chattersName = new HashSet<string>();

                foreach (var chatterGroup in chatters)
                {
                    var chattersArrayProp = chatterGroup as JProperty;
                    if (chattersArrayProp == null)
                        continue;

                    var chattersArray = chattersArrayProp.Value as JArray;
                    if (chattersArray == null)
                        continue;

                    foreach (var chatter in chattersArray)
                        if (chatter.Type == JTokenType.String)
                            chattersName.Add(chatter.ToString());
                }

                List<UpdateOneModel<TwitchTrackedUser>> bulkOps = new List<UpdateOneModel<TwitchTrackedUser>>();

                foreach (var chatter in chattersName)
                {
                    bulkOps.Add(new UpdateOneModel<TwitchTrackedUser>(new BsonDocument { { "DisplayName", chatter }, { "BroadcasterId", channel.BroadcasterId } }, new BsonDocument { { "$set", new BsonDocument("LastVisited", currentTime) }, { "$inc", new BsonDocument("Count", 1) } }) { IsUpsert = true });

                    if (bulkOps.Count > 10000)
                    {
                        await trackedUsersCollection.BulkWriteAsync(bulkOps);
                        bulkOps.Clear();
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await trackedUsersCollection.BulkWriteAsync(bulkOps);
                    bulkOps.Clear();
                }

            }
            catch (Exception es) { Console.WriteLine($"{channel.BroadcasterId} {(string.IsNullOrEmpty(channel.BroadcasterName) ? "" : channel.BroadcasterName)} {(string.IsNullOrEmpty(channel.BroadcasterLogin) ? "" : channel.BroadcasterLogin)}"); Console.WriteLine(es); }
        }

        private void RemoveCompletedTasks(Dictionary<string, Task> dict)
        {
            var tasksRemoveKeyList = new List<string>();
            foreach (var pair in dict)
                if (pair.Value.IsCompleted)
                    tasksRemoveKeyList.Add(pair.Key);

            foreach (var key in tasksRemoveKeyList)
                dict.Remove(key);
        }

        private async Task UpdateChannelsChattersLoop(TimeSpan period)
        {
            var currentUpdaterTasks = new Dictionary<string, Task>();

            while (!cancellationToken.Token.IsCancellationRequested)
            {
                var startTime = Environment.TickCount64;

                var currentTimeBeforeUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var lastHour = (currentTimeBeforeUpdate - Math.Max(period.Seconds, TrackerConfiguration.LastLiveCheckTime));
                var channelsCount = 0;
                var nextChannelUpdate = currentTimeBeforeUpdate + 60;

                RemoveCompletedTasks(currentUpdaterTasks);

                await (await trackedChannelsCollection.FindAsync(x => x.LastCheckLiveTime > lastHour)).ForEachAsync(async x =>
                {
                    if (currentUpdaterTasks.ContainsKey(x.BroadcasterId))
                        return;

                    if (_lastChannelsUpdates.TryGetValue(x.BroadcasterId, out long lastCheck))
                    {
                        var deltaCheck = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - lastCheck;

                        if (deltaCheck < period.TotalSeconds)
                        {
                            var nextCheck = lastCheck + (long)period.TotalSeconds;

                            if (nextCheck < nextChannelUpdate)
                                nextChannelUpdate = nextCheck;

                            return;
                        }

                        if (deltaCheck - period.TotalSeconds > period.TotalSeconds)
                            Console.WriteLine($"Critical error for {x.BroadcasterLogin} elapsed {deltaCheck}s");
                    }

                    while (currentUpdaterTasks.Count >= TrackerConfiguration.MaxUpdaterTasks)
                    {
                        try
                        {
                            await Task.Delay(50, cancellationToken.Token);
                        }
                        catch (OperationCanceledException) { }
                        RemoveCompletedTasks(currentUpdaterTasks);
                    }

                    channelsCount++;
                    currentUpdaterTasks[x.BroadcasterId] = Task.Run(() => UpdateChannelChatters(x));
                });

                var deltaTimeTasks = nextChannelUpdate - DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                await Task.WhenAny(Task.WhenAll(currentUpdaterTasks.Values), Task.Delay((int)Math.Max(100, deltaTimeTasks * 1000)));

                var endTime = Environment.TickCount64;
                var delta = endTime - startTime;

                if (channelsCount > 0)
                    Console.WriteLine($"UpdateChannelsChattersLoop end {delta}ms for {channelsCount} channels current tasks {currentUpdaterTasks.Count}");

                try
                {
                    var currentTimeAfterLoop = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                    if (nextChannelUpdate > currentTimeAfterLoop)
                    {
                        Console.WriteLine($"Next update {nextChannelUpdate - currentTimeAfterLoop}s");
                        await Task.Delay((int)((nextChannelUpdate - currentTimeAfterLoop) * 1000) + 1000, cancellationToken.Token);
                    }
                    else
                        Console.WriteLine("Next update instant");
                }
                catch (OperationCanceledException) { }
            }
        }

        private void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            cancellationToken.Cancel();

            Console.WriteLine("Exiting...");

            File.WriteAllText("channels_updates.json", JsonConvert.SerializeObject(_lastChannelsUpdates));

            e.Cancel = true;
        }

        private async Task MainLoop()
        {
            cancellationToken = new CancellationTokenSource();

            {
                var polishWords = Properties.Resources.polish.Split(new string[] { "\r\n" }, StringSplitOptions.None);
                foreach (var word in polishWords)
                    polishWordList.Add(new string(removePolishCharacters(word.ToLower().Trim().ToCharArray())));
            }

            Console.CancelKeyPress += Console_CancelKeyPress;

            if (File.Exists("channels_updates.json"))
                _lastChannelsUpdates = JsonConvert.DeserializeObject<ConcurrentDictionary<string, long>>(await File.ReadAllTextAsync("channels_updates.json"));

            var mongoClient = new MongoClient(TrackerConfiguration.MongoConnectionUrl);
            mongoDatabase = mongoClient.GetDatabase("twitchusertracker");
            
            {
                trackedChannelsCollection = mongoDatabase.GetCollection<TwitchTrackedChannel>("trackedchannels");
                trackedUsersCollection = mongoDatabase.GetCollection<TwitchTrackedUser>("trackedusers");
            
                await trackedChannelsCollection.Indexes.CreateOneAsync(new CreateIndexModel<TwitchTrackedChannel>(Builders<TwitchTrackedChannel>.IndexKeys.Ascending(x => x.LastCheckLiveTime)));
                await trackedChannelsCollection.Indexes.CreateOneAsync(new CreateIndexModel<TwitchTrackedChannel>(Builders<TwitchTrackedChannel>.IndexKeys.Ascending(x => x.BroadcasterId), new CreateIndexOptions() { Unique = true }));
            
                await trackedUsersCollection.Indexes.CreateOneAsync(new CreateIndexModel<TwitchTrackedUser>(Builders<TwitchTrackedUser>.IndexKeys.Ascending(x => x.DisplayName).Ascending(x => x.BroadcasterId), new CreateIndexOptions() { Unique = true }));
            }

            _httpClient.DefaultRequestHeaders.Add("Client-Id", "kimne78kx3ncx6brgo4mv6wki5h1ko");

            currentTasks.Add(Task.Run(UpdateLiveChannelToTracksLoop));

            try
            {
                await Task.Delay(-1, cancellationToken.Token);
            }
            catch (OperationCanceledException) { }
            await Task.WhenAll(currentTasks);
        }
    }
}