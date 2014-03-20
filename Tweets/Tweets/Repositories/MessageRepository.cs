using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Linq;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class MessageRepository : IMessageRepository
    {
        private readonly string connectionString;
        private readonly AttributeMappingSource mappingSource;
        private readonly IMapper<Message, MessageDocument> messageDocumentMapper;

        public MessageRepository(IMapper<Message, MessageDocument> messageDocumentMapper)
        {
            this.messageDocumentMapper = messageDocumentMapper;
            mappingSource = new AttributeMappingSource();
            connectionString = ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
        }

        public void Save(Message message)
        {
            var messageDocument = messageDocumentMapper.Map(message);
            using (var dc = new DataContext(connectionString, mappingSource))
            {
                dc.GetTable<MessageDocument>().InsertOnSubmit(messageDocument);
                dc.SubmitChanges();
            }
        }

        public void Like(Guid messageId, User user)
        {
            var likeDocument = new LikeDocument {MessageId = messageId, UserName = user.Name, CreateDate = DateTime.UtcNow};
            using (var dc = new DataContext(connectionString, mappingSource))
            {
                dc.GetTable<LikeDocument>().InsertOnSubmit(likeDocument);
                dc.SubmitChanges();
            }
        }

        public void Dislike(Guid messageId, User user)
        {
            using (var dc = new DataContext(connectionString, mappingSource))
            {
                var delete = dc.GetTable<LikeDocument>().Where(x => x.MessageId == messageId && x.UserName == user.Name);
                dc.GetTable<LikeDocument>().DeleteAllOnSubmit(delete);
                dc.SubmitChanges();
            }
        }

        public IEnumerable<Message> GetPopularMessages()
        {
            using (var dc = new DataContext(connectionString, mappingSource))
            {
                return dc.GetTable<MessageDocument>()
                    .GroupJoin(dc.GetTable<LikeDocument>(), x => x.Id, x => x.MessageId, (x, y) => new { Message = x, Likes = y })
                    .SelectMany(x => x.Likes.DefaultIfEmpty(), (x, y) => new { x.Message, Like = y })
                    .GroupBy(x => x.Message)
                    .Select(x => new { Message = x.Key, Likes = x.Count(y => y.Like != null) })
                    .OrderByDescending(x => x.Likes)
                    .Take(10)
                    .Select(x => new Message
                    {
                        Id = x.Message.Id,
                        User = new User { Name = x.Message.UserName },
                        Text = x.Message.Text,
                        CreateDate = x.Message.CreateDate,
                        Likes = x.Likes,
                    })
                    .ToArray();
            }
        }

        public IEnumerable<UserMessage> GetMessages(User user)
        {
            using (var dc = new DataContext(connectionString, mappingSource))
            {
                return dc.GetTable<MessageDocument>().Where(x => x.UserName == user.Name)
                    .GroupJoin(dc.GetTable<LikeDocument>(), x => x.Id, x => x.MessageId, (x, y) => new { Message = x, Likes = y })
                    .SelectMany(x => x.Likes.DefaultIfEmpty(), (x, y) => new { x.Message, Like = y })
                    .GroupBy(x => x.Message)
                    .Select(x => new UserMessage
                    {
                        Id = x.Key.Id,
                        User = new User { Name = x.Key.UserName },
                        Text = x.Key.Text,
                        CreateDate = x.Key.CreateDate,
                        Liked = x.Any(y => y.Like.UserName == user.Name),
                        Likes = x.Count(y => y.Like != null),
                    })
                    .OrderByDescending(x => x.CreateDate)
                    .ToArray();
            }
        }
    }
}
