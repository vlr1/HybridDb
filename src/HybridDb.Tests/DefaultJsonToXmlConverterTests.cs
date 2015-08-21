using System.Collections.Generic;
using System.Xml.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace HybridDb.Tests
{
    //todo: missing support for json.net's $type and $ref fields, consider not supporting custom json.net settings
    //todo: missing support for types/discriminators

    public class DefaultJsonToXmlConverterTests
    {
        [Fact]
        public void ConvertsTheEmptyObject()
        {
            Assert.Equal(
                new XDocument(
                    new XElement("root")),
                new DefaultJsonToXmlConverter().Convert("{}"),
                XNode.EqualityComparer);
        }

        [Fact]
        public void ConvertsObject()
        {
            var json = JObject.FromObject(new
            {
                value = 2,
                str = "asger",
            });

            Assert.Equal(
                new XDocument(
                    new XElement("root",
                        new XElement("value", 2),
                        new XElement("str", "asger"))),
                new DefaultJsonToXmlConverter().Convert(json.ToString()),
                XNode.EqualityComparer);
        }

        [Fact]
        public void ConvertsEnumerable()
        {
            var json = JArray.FromObject(new List<object>
            {
                "asger",
                new
                {
                    value = 1
                }
            });

            Assert.Equal(
                new XDocument(
                    new XElement("root",
                        new XElement("item", "asger"),
                        new XElement("item",
                            new XElement("value", 1)))),
                new DefaultJsonToXmlConverter().Convert(json.ToString()),
                XNode.EqualityComparer);
        }

        [Fact]
        public void ConvertsEnumerableWithASingleItem()
        {
            var json = JArray.FromObject(new List<string> { "asger" });

            Assert.Equal(
                new XDocument(
                    new XElement("root",
                        new XElement("item", "asger"))),
                new DefaultJsonToXmlConverter().Convert(json.ToString()),
                XNode.EqualityComparer);
        }

        [Fact]
        public void ConvertsNestedObjectsAndEnumerables()
        {
            var json = JObject.FromObject(
                new
                {
                    Value = 1,
                    NestedList = new List<string> {"asger", "karin"},
                    NestedObject = new
                    {
                        Value = 2,
                        EvenMoreNesting = new List<object>
                        {
                            new
                            {
                                Value = 3
                            }
                        }
                    }

                });

            Assert.Equal(
                new XDocument(
                    new XElement("root",
                        new XElement("Value", 1),
                        new XElement("NestedList",
                            new XElement("item", "asger"),
                            new XElement("item", "karin")),
                        new XElement("NestedObject",
                            new XElement("Value", 2),
                            new XElement("EvenMoreNesting",
                                new XElement("item",
                                    new XElement("Value", 3)))))),
                new DefaultJsonToXmlConverter().Convert(json.ToString()),
                XNode.EqualityComparer);
        }
    }
}