﻿using System;

namespace HybridDb
{
    public abstract class Window { }

    public class SkipToId : Window
    {
        public SkipToId(Guid id, int pageSize)
        {
            Id = id;
            PageSize = pageSize;
        }

        public Guid Id { get; }
        public int PageSize { get; }
    }

    public class SkipTake : Window
    {
        public SkipTake(int skip, int take)
        {
            Skip = skip;
            Take = take;
        }

        public int Skip { get; }
        public int Take { get; }
    }

}