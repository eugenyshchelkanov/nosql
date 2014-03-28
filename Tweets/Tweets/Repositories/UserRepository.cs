using System;
using System.Reflection;
using CorrugatedIron;
using CorrugatedIron.Models;
using Tweets.Attributes;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class UserRepository : IUserRepository
    {
        private readonly string bucketName;
        private readonly IRiakClient riakClient;
        private readonly IMapper<User, UserDocument> userDocumentMapper;
        private readonly IMapper<UserDocument, User> userMapper;

        public UserRepository(IRiakClient riakClient, IMapper<User, UserDocument> userDocumentMapper, IMapper<UserDocument, User> userMapper)
        {
            this.riakClient = riakClient;
            this.userDocumentMapper = userDocumentMapper;
            this.userMapper = userMapper;
            bucketName = typeof (UserDocument).GetCustomAttribute<BucketNameAttribute>().BucketName;
        }

        public void Save(User user)
        {
            var document = userDocumentMapper.Map(user);
            var result = riakClient.Async.Put(new RiakObject(bucketName, document.Id, document)).Result;
            if (!result.IsSuccess)
                throw new Exception(result.ErrorMessage);
        }

        public User Get(string userName)
        {
            var result = riakClient.Async.Get(bucketName, userName).Result;

            if (result.ResultCode == ResultCode.NotFound)
                return null;

            if (!result.IsSuccess)
                throw new Exception(result.ErrorMessage);

            var document = result.Value.GetObject<UserDocument>();
            return userMapper.Map(document);
        }
    }
}