﻿using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Infra.NServiceBus.Inbox
{
    public class InboxRecord
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string ContentId { get; set; }
        public long ContentVersion { get; set; }
        public string CheckMessageOrderType { get; set; }
        public Guid MessageId { get; set; }
        public DateTime ModifiedAtUtc { get; set; }
    }
}
