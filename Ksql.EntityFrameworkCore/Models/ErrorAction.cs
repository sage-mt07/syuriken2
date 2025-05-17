using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ksql.EntityFrameworkCore.Models;

public enum ErrorAction
{
    Stop,
    Skip,
    LogAndContinue,
    DeadLetterQueue
}