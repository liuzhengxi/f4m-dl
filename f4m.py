#!/usr/bin/env python

import xml.etree.ElementTree as etree
import base64
from struct import unpack, pack
import sys
import io
import os

import youtube_dl
from youtube_dl.utils import *


class FlvReader(io.BytesIO):
    """
    Reader for Flv files
    The file format is documented in https://www.adobe.com/devnet/f4v.html
    """

    # Utility functions for reading numbers and strings
    def read_unsigned_long_long(self):
        return unpack('!Q', self.read(8))[0]
    def read_unsigned_int(self):
        return unpack('!I', self.read(4))[0]
    def read_unsigned_char(self):
        return unpack('!B', self.read(1))[0]
    def read_string(self):
        res = b''
        while True:
            char = self.read(1)
            if char == b'\x00':
                break
            res+=char
        return res

    def read_box_info(self):
        """
        Read a box and return the info as a tuple: (box_size, box_type, box_data)
        """
        real_size = size = self.read_unsigned_int()
        box_type = self.read(4)
        header_end = 8
        if size == 1:
            real_size = self.read_unsigned_long_long()
            header_end = 16
        return real_size, box_type, self.read(real_size-header_end)

    def read_asrt(self, debug=False):
        version = self.read_unsigned_char()
        self.read(3) # flags
        quality_entry_count = self.read_unsigned_char()
        quality_modifiers = []
        for i in range(quality_entry_count):
            quality_modifier = self.read_string()
            quality_modifiers.append(quality_modifier)
        segment_run_count = self.read_unsigned_int()
        segments = []
        for i in range(segment_run_count):
            first_segment = self.read_unsigned_int()
            fragments_per_segment = self.read_unsigned_int()
            segments.append((first_segment, fragments_per_segment))

        return {'version': version,
                'quality_segment_modifiers': quality_modifiers,
                'segment_run': segments,
                }

    def read_afrt(self, debug=False):
        version = self.read_unsigned_char()
        self.read(3) # flags
        time_scale = self.read_unsigned_int()
        quality_entry_count = self.read_unsigned_char()
        quality_entries = []
        for i in range(quality_entry_count):
            mod = self.read_string()
            quality_entries.append(mod)
        fragments_count = self.read_unsigned_int()
        fragments = []
        for i in range(fragments_count):
            first = self.read_unsigned_int()
            first_ts = self.read_unsigned_long_long()
            duration = self.read_unsigned_int()
            if duration == 0:
                discontinuity_indicator = self.read_unsigned_char()
            else:
                discontinuity_indicator = None
            fragments.append({'first': first,
                              'ts': first_ts,
                              'duration': duration,
                              'discontinuity_indicator': discontinuity_indicator,
                              })

        return {'version': version,
                'time_scale': time_scale,
                'fragments': fragments,
                }

    def read_abst(self, debug=False):
        version = self.read_unsigned_char()
        self.read(3) # flags
        bootstrap_info_version = self.read_unsigned_int()
        self.read(1) # Profile,Live,Update,Reserved
        time_scale = self.read_unsigned_int()
        current_media_time = self.read_unsigned_long_long()
        smpteTimeCodeOffset = self.read_unsigned_long_long()
        movie_identifier = self.read_string()
        server_count = self.read_unsigned_char()
        servers = []
        for i in range(server_count):
            server = self.read_string()
            servers.append(server)
        quality_count = self.read_unsigned_char()
        qualities = []
        for i in range(server_count):
            quality = self.read_string()
            qualities.append(server)
        drm_data = self.read_string()
        metadata = self.read_string()
        segments_count = self.read_unsigned_char()
        segments = []
        for i in range(segments_count):
            box_size, box_type, box_data = self.read_box_info()
            assert box_type == b'asrt'
            segment = FlvReader(box_data).read_asrt()
            segments.append(segment)
        fragments_run_count = self.read_unsigned_char()
        fragments = []
        for i in range(fragments_run_count):
            # This info is only useful for the player, it doesn't give more info 
            # for the download process
            box_size, box_type, box_data = self.read_box_info()
            assert box_type == b'afrt'
            fragments.append(FlvReader(box_data).read_afrt())
    
        return {'segments': segments,
                'movie_identifier': movie_identifier,
                'drm_data': drm_data,
                }

    def read_bootstrap_info(self):
        """
        Read the bootstrap information from the stream,
        returns a dict with the following keys:
        segments: A list of dicts with the following keys
            segment_run: A list of (first_segment, fragments_per_segment) tuples
        """
        total_size, box_type, box_data = self.read_box_info()
        assert box_type == b'abst'
        return FlvReader(box_data).read_abst()


def _add_ns(prop):
    return '{http://ns.adobe.com/f4m/1.0}%s' % prop


class F4MDownloader(youtube_dl.FileDownloader):
    """
    A downloader for f4m manifests or AdobeHDS.
    """

    def to_screen(self, msg, prefix=False, *args, **kargs):
        if prefix:
            msg = u'[download] %s' % msg
        super(F4MDownloader, self).to_screen(msg, *args, **kargs)

    def _write_flv_header(self, stream, metadata):
        """Writes the FLV header and the metadata to stream"""
        # FLV header
        stream.write(b'FLV\x01')
        stream.write(b'\x05')
        stream.write(b'\x00\x00\x00\x09')
        # FLV File body
        stream.write(b'\x00\x00\x00\x00')
        # FLVTAG
        stream.write(b'\x12') # Script data
        stream.write(pack('!L',len(metadata))[1:]) # Size of the metadata with 3 bytes
        stream.write(b'\x00\x00\x00\x00\x00\x00\x00')
        stream.write(metadata)
        # All this magic numbers have been extracted from the output file
        # produced by AdobeHDS.php (https://github.com/K-S-V/Scripts)
        stream.write(b'\x00\x00\x01\x73')

    def download_info_dict(self, filename, info_dict):
        man_url = info_dict['url']
        self.to_screen(u'Downloading f4m manifest', True)
        manifest = compat_urllib_request.urlopen(man_url).read()
        self.report_destination(filename)
        dl = youtube_dl.FileDownloader(self.ydl, {'continuedl': True})

        doc = etree.fromstring(manifest)
        formats = [(int(f.attrib.get('bitrate', -1)),f) for f in doc.findall(_add_ns('media'))]
        formats = sorted(formats, key=lambda f: f[0])
        rate, media = formats[0]
        base_url = compat_urlparse.urljoin(man_url,media.attrib['url'])
        bootstrap = base64.b64decode(doc.find(_add_ns('bootstrapInfo')).text)
        metadata = base64.b64decode(media.find(_add_ns('metadata')).text)
        boot_info = FlvReader(bootstrap).read_bootstrap_info()

        tmpfilename = self.temp_name(filename)
        (dest_stream, tmpfilename) = sanitize_open(tmpfilename, 'wb')
        self._write_flv_header(dest_stream, metadata)

        self.to_screen(u'Downloading %d segments' % len(boot_info['segments']), True)
        frags_filenames = []
        for seg_i, seg in enumerate(boot_info['segments'],1):
            n_frags = seg['segment_run'][0][1]
            self.to_screen(u'Downloading %d fragments' % n_frags, True)
            for frag_i in range(1, n_frags+1):
                name = u'Seg%d-Frag%d' % (seg_i, frag_i)
                url = base_url + name
                frag_filename = u'%s-%s' % (tmpfilename, name)
                success = dl._do_download(frag_filename, {'url': url})
                if not success:
                    return False
                with open(frag_filename, 'rb') as down:
                    down_data = down.read()
                    reader = FlvReader(down_data)
                    for i in range(10):
                        _, box_type, box_data = reader.read_box_info()
                        if box_type == b'mdat':
                            dest_stream.write(box_data)
                            break
                            # Using the following code may fix some videos, but 
                            # only in mplayer, VLC won't play the sound.
                            # mdat_reader = FlvReader(box_data)
                            # media_type = mdat_reader.read_unsigned_char()
                            # while True:
                            #     if mdat_reader.read_unsigned_char() == media_type:
                            #         if mdat_reader.read_unsigned_char() == 0x00:
                            #             break
                            # dest_stream.write(pack('!B', media_type))
                            # dest_stream.write(b'\x00')
                            # dest_stream.write(mdat_reader.read())
                            # break
                frags_filenames.append(frag_filename)

        self.try_rename(tmpfilename, filename)
        for frag_file in frags_filenames:
            os.remove(frag_file)

if __name__ == '__main__':
    ydl = youtube_dl.YoutubeDL({'outtmpl': ''})
    f4m_dl = F4MDownloader(ydl, {})
    f4m_dl.download_info_dict(u'test1.flv', {'url': sys.argv[1]})
