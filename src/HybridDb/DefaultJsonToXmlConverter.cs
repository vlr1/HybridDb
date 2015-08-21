using System;
using System.Linq;
using System.Xml.Linq;
using Newtonsoft.Json.Linq;

namespace HybridDb
{
    public class DefaultJsonToXmlConverter : IJsonToXmlConverter
    {
        public XDocument Convert(string json)
        {
            return new XDocument(Parse("root", JToken.Parse(json)));
        }

        public XElement Parse(string name, JToken token)
        {
            var jObject = token as JObject;
            if (jObject != null)
            {
                return new XElement(name,
                    from property in jObject.Properties()
                    select Parse(property.Name, property.Value));
            }

            var jArray = token as JArray;
            if (jArray != null)
            {
                return new XElement(name, from item in jArray select Parse("item", item));
            }

            var jValue = token as JValue;
            if (jValue != null)
            {
                return new XElement(name, jValue.Value);
            }

            throw new InvalidOperationException();
        }
    }
}