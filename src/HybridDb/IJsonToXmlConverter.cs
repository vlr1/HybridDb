using System.Xml.Linq;

namespace HybridDb
{
    public interface IJsonToXmlConverter
    {
        XDocument Convert(string json);
    }
}