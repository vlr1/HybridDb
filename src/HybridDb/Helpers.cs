using System.Collections.Generic;

namespace HybridDb
{
    public static class Helpers
    {
        public static IReadOnlyList<T> ListOf<T>(params T[] list) => list;
    }
}