create table messages (id uniqueidentifier primary key not null, userName varchar(100), text varchar(1000), createDate datetime, version rowversion not null)
create table likes (userName varchar(100), messageId uniqueidentifier not null, createDate datetime, primary key (userName, messageId), foreign key (messageId) references messages(id));
