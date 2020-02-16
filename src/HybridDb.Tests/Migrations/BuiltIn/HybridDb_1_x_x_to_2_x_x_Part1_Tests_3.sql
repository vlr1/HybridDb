/****** Object:  Table [dbo].[BuildingParts]    Script Date: 20-01-2020 14:09:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[BuildingParts](
	[Id] [nvarchar](1024) NOT NULL,
	[Etag] [uniqueidentifier] NOT NULL,
	[CreatedAt] [datetimeoffset](7) NOT NULL,
	[ModifiedAt] [datetimeoffset](7) NOT NULL,
	[Document] [varbinary](max) NULL,
	[Metadata] [varbinary](max) NULL,
	[Discriminator] [nvarchar](1024) NULL,
	[AwaitsReprojection] [bit] NOT NULL,
	[Version] [int] NOT NULL,
	[Litra] [nvarchar](1024) NULL,
	[Name] [nvarchar](1024) NULL,
	[UValue] [decimal](28, 14) NOT NULL,
	[OriginBuildingPartId] [uniqueidentifier] NULL,
	[UValueForHB2019] [decimal](28, 14) NOT NULL,
	[ExtentPercentage] [int] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[HybridDb]    Script Date: 20-01-2020 14:09:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[HybridDb](
	[SchemaVersion] [int] NOT NULL
) ON [PRIMARY]

GO
INSERT [dbo].[BuildingParts] ([Id], [Etag], [CreatedAt], [ModifiedAt], [Document], [Metadata], [Discriminator], [AwaitsReprojection], [Version], [Litra], [Name], [UValue], [OriginBuildingPartId], [UValueForHB2019], [ExtentPercentage]) VALUES (N'00beabae-e0b9-4cbc-a6e9-4891f10b88d2', N'c706dfb8-8713-4d7a-9e28-fc603f7fc22a', CAST(N'2018-11-07T14:32:41.0229768+01:00' AS DateTimeOffset), CAST(N'2019-08-29T21:42:35.5453204+02:00' AS DateTimeOffset), 0x340B00000224696400020000003100054964001000000004AEABBE00B9E0BC4CA6E94891F10B88D210457874656E7450657263656E74616765000000000002527365001B000000426173656D656E7457616C6C734F6E654D6574657242656C6F7700025273690013000000486F72697A6F6E74616C48656174466C6F77000245787465726E616C4E6F7465005800000042657265676E696E67656E20657220756466C3B872742065667465722044532034313820372E207564676176653A2032303131202D2042657265676E696E67206166206279676E696E67657273207661726D657461622E000A496E7465726E616C4E6F746500024C6974726100310000004BC3A66C64657276C3A66720302D326D202D20333920636D206C65746B6C696E6B6572202D203130306D6D2D7564762E00024E616D65004F0000004BC3A66C6465727964657276C3A66720302D326D206479626465202D20333920636D206C65746B6C696E6B65726265746F6E202D20313030206D6D20756476656E6469672069736F6C6572696E6700034F726967696E4275696C64696E675061727400940900000224696400020000003200022474797065003F0000005556616C756543616C63756C61746F722E4D6F64656C732E53706563696669634275696C64696E67506172742C205556616C756543616C63756C61746F7200054964001000000004993EF20356490841B3308F6E431B335E10457874656E7450657263656E7461676500000000000252736500080000004F75747369646500025273690013000000486F72697A6F6E74616C48656174466C6F77000245787465726E616C4E6F7465005800000042657265676E696E67656E20657220756466C3B872742065667465722044532034313820372E207564676176653A2032303131202D2042657265676E696E67206166206279676E696E67657273207661726D657461622E000A496E7465726E616C4E6F746500024C69747261001E000000333920636D206C65746B6C696E6B6572202D203130306D6D2D7564762E00024E616D6500440000004D6173736976207964657276C3A667202D20333920636D206C65746B6C696E6B65726265746F6E202D20313030206D6D20756476656E6469672069736F6C6572696E670003436F7272656374696F6E7342696E64696E6773001E0000000224696400020000003300042476616C75657300050000000000034D6174657269616C42696E64696E677300AA0700000224696400020000003400042476616C75657300910700000330007A010000022469640002000000350002247479706500450000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C42696E64696E672C205556616C756543616C63756C61746F72000542696E64696E674964001000000004DDAE23B3F24A0C4597744FAE2315D0320144696D656E73696F6E00B81E85EB51B88E3F02457874656E74537461747573000C0000004E6F74496E636C756465640002496E73756C6174696F6E41697247617000070000004E6F7453657400034D6174657269616C00A20000000224696400020000003600022474797065003E0000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C2C205556616C756543616C63756C61746F7200054964001000000004AFF1ED3A1CAA3D4696A6AEDFF8FFF29B014C616D62646100000000000000D03F024E616D650016000000496E6476656E6469672062656B6CC3A6646E696E670000000331008D010000022469640002000000370002247479706500450000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C42696E64696E672C205556616C756543616C63756C61746F72000542696E64696E674964001000000004C45EEC1DDCD0FC48B2FAAFB3E4F9115A0144696D656E73696F6E008FC2F5285C8FD23F02457874656E74537461747573000C0000004E6F74496E636C756465640002496E73756C6174696F6E41697247617000070000004E6F7453657400034D6174657269616C00B50000000224696400020000003800022474797065003E0000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C2C205556616C756543616C63756C61746F7200054964001000000004C94E421BE2EEFE4BA7BB02AFF5CEDE97014C616D62646100EC51B81E85EBD13F024E616D6500290000004C65746B6C696E6B65726265746F6E2C20696E6476656E646967742028383030206B672F6DC2B3290000000332008D010000022469640002000000390002247479706500450000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C42696E64696E672C205556616C756543616C63756C61746F72000542696E64696E67496400100000000485C018F1D9423A418A6182B52D4569350144696D656E73696F6E009A9999999999B93F02457874656E74537461747573000C0000004E6F74496E636C756465640002496E73756C6174696F6E41697247617000070000004E6F7453657400034D6174657269616C00B5000000022469640003000000313000022474797065003E0000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C2C205556616C756543616C63756C61746F720005496400100000000412917D85A8B0C14CA3D7C6846754B080014C616D62646100D7A3703D0AD7D33F024E616D6500280000004C65746B6C696E6B65726265746F6E2C20756476656E646967742028383030206B672F6DC2B3290000000333007E01000002246964000300000031310002247479706500450000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C42696E64696E672C205556616C756543616C63756C61746F72000542696E64696E67496400100000000487F3C1E2669BED4DAACB8CD688FCB9F30144696D656E73696F6E009A9999999999B93F02457874656E74537461747573000C0000004E6F74496E636C756465640002496E73756C6174696F6E41697247617000070000004C6576656C3100034D6174657269616C00A5000000022469640003000000313200022474797065003E0000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C2C205556616C756543616C63756C61746F7200054964001000000004AC6628DD91F9834ABDBF6309DCD0A2AB014C616D626461009CC420B07268A13F024E616D65001800000048C3A572642069736F6C6572696E672C206B6C2E2033340000000334006B01000002246964000300000031330002247479706500450000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C42696E64696E672C205556616C756543616C63756C61746F72000542696E64696E67496400100000000439F3A2DE5B90ED46BA0E59C4189BC85D0144696D656E73696F6E007B14AE47E17A743F02457874656E74537461747573000C0000004E6F74496E636C756465640002496E73756C6174696F6E41697247617000070000004E6F7453657400034D6174657269616C0092000000022469640003000000313400022474797065003E0000005556616C756543616C63756C61746F722E4D6F64656C732E44696D656E73696F6E65644D6174657269616C2C205556616C756543616C63756C61746F72000549640010000000040AFE915A8D36B2429E6CB2489F13FCF9014C616D62646100CDCCCCCCCCCCF03F024E616D6500050000005075647300000000000000, NULL, N'UValueCalculator.Models.RefBuildingPart, UValueCalculator, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null', 0, 0, N'Kældervæg 0-2m - 39 cm letklinker - 100mm-udv.', N'Kælderydervæg 0-2m dybde - 39 cm letklinkerbeton - 100 mm udvendig isolering', CAST(0.20369913712122 AS Decimal(28, 14)), N'03f23e99-4956-4108-b330-8f6e431b335e', CAST(0.20369913712122 AS Decimal(28, 14)), 0)
GO
INSERT [dbo].[HybridDb] ([SchemaVersion]) VALUES (0)
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('') FOR [Id]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('00000000-0000-0000-0000-000000000000') FOR [Etag]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('01-01-0001 00:00:00 +00:00') FOR [CreatedAt]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('01-01-0001 00:00:00 +00:00') FOR [ModifiedAt]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('False') FOR [AwaitsReprojection]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('0') FOR [Version]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('0') FOR [UValue]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('00000000-0000-0000-0000-000000000000') FOR [OriginBuildingPartId]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('0') FOR [UValueForHB2019]
GO
ALTER TABLE [dbo].[BuildingParts] ADD  DEFAULT ('0') FOR [ExtentPercentage]
GO
ALTER TABLE [dbo].[HybridDb] ADD  DEFAULT ('0') FOR [SchemaVersion]
GO
