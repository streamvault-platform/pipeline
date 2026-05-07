package io.streamvault.pipeline.domain

import zio.json.*
import zio.test.*

import java.util.UUID

object EventSerializationSpec extends ZIOSpecDefault:

  private val id = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")

  def spec = suite("Event JSON serialization")(

    suite("TrackUploadedEvent")(
      test("decodes a complete valid payload") {
        val json =
          s"""{
             |  "trackId":             "$id",
             |  "filePath":            "/tracks/song.mp3",
             |  "mimeType":            "audio/mpeg",
             |  "originalFilename":    "song.mp3",
             |  "downloadUrl":         "https://storage.example.com/dl?sig=abc",
             |  "uploadUrl":           "https://storage.example.com/ul?sig=xyz",
             |  "transcodedStoredPath":"tracks/transcoded/song.aac"
             |}""".stripMargin
        val result = json.fromJson[TrackUploadedEvent]
        assertTrue(result.isRight) &&
        assertTrue(result.toOption.get.trackId == id) &&
        assertTrue(result.toOption.get.originalFilename == "song.mp3") &&
        assertTrue(result.toOption.get.transcodedStoredPath == "tracks/transcoded/song.aac")
      },
      test("fails when a required field is missing") {
        val json = s"""{"trackId":"$id","filePath":"/f","mimeType":"audio/mpeg","originalFilename":"f.mp3","downloadUrl":"http://x","uploadUrl":"http://y"}"""
        assertTrue(json.fromJson[TrackUploadedEvent].isLeft)
      },
      test("fails when trackId is not a valid UUID") {
        val json = """{"trackId":"not-a-uuid","filePath":"/f","mimeType":"audio/mpeg","originalFilename":"f.mp3","downloadUrl":"http://x","uploadUrl":"http://y","transcodedStoredPath":"p"}"""
        assertTrue(json.fromJson[TrackUploadedEvent].isLeft)
      },
      test("ignores unknown fields (forward-compatibility)") {
        val json =
          s"""{
             |  "trackId":             "$id",
             |  "filePath":            "/f",
             |  "mimeType":            "audio/mpeg",
             |  "originalFilename":    "f.mp3",
             |  "downloadUrl":         "http://x",
             |  "uploadUrl":           "http://y",
             |  "transcodedStoredPath":"p",
             |  "futureField":         "ignored"
             |}""".stripMargin
        assertTrue(json.fromJson[TrackUploadedEvent].isRight)
      }
    ),

    suite("MetadataReadyEvent")(
      test("encodes with camelCase field names matching the core contract") {
        val event = MetadataReadyEvent(
          trackId     = id,
          title       = Some("My Song"),
          artist      = Some("Artist Name"),
          album       = Some("My Album"),
          year        = Some(2024),
          trackNumber = Some(3),
          discNumber  = Some(1),
          durationMs  = Some(180000),
          genre       = Some("Rock")
        )
        val json = event.toJson
        assertTrue(json.contains("\"trackId\"")) &&
        assertTrue(json.contains("\"trackNumber\"")) &&
        assertTrue(json.contains("\"discNumber\"")) &&
        assertTrue(json.contains("\"durationMs\"")) &&
        assertTrue(!json.contains("track_id")) &&
        assertTrue(!json.contains("track_number"))
      },
      test("omits None optional fields entirely (no null values)") {
        val event = MetadataReadyEvent(
          trackId = id, title = None, artist = None, album = None,
          year = None, trackNumber = None, discNumber = None,
          durationMs = None, genre = None
        )
        val json = event.toJson
        assertTrue(!json.contains("\"title\"")) &&
        assertTrue(!json.contains("null"))
      },
      test("roundtrips through encode → decode") {
        val event = MetadataReadyEvent(
          trackId = id, title = Some("T"), artist = Some("A"), album = Some("B"),
          year = Some(2020), trackNumber = Some(1), discNumber = Some(1),
          durationMs = Some(9000), genre = Some("Jazz")
        )
        assertTrue(event.toJson.fromJson[MetadataReadyEvent] == Right(event))
      }
    ),

    suite("TranscodedEvent")(
      test("encodes all required fields with correct names") {
        val event = TranscodedEvent(
          trackId          = id,
          transcodedPath   = "tracks/transcoded/song.aac",
          mimeType         = "audio/aac",
          fileSizeBytes    = 1234567L
        )
        val json = event.toJson
        assertTrue(json.contains("\"trackId\"")) &&
        assertTrue(json.contains("\"transcodedPath\"")) &&
        assertTrue(json.contains("\"fileSizeBytes\"")) &&
        assertTrue(json.contains("\"audio/aac\""))
      },
      test("roundtrips through encode → decode") {
        val event = TranscodedEvent(id, "path/file.aac", "audio/aac", 99999L)
        assertTrue(event.toJson.fromJson[TranscodedEvent] == Right(event))
      }
    )
  )
