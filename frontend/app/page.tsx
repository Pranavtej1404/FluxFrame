'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { uploadVideo, getJobs } from '@/lib/api';

export default function Home() {
  const [file, setFile] = useState<File | null>(null);
  const [jobs, setJobs] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchJobs();
  }, []);

  const fetchJobs = async () => {
    try {
      const data = await getJobs();
      setJobs(data);
    } catch (e) {
      console.error(e);
    }
  };

  const handleUpload = async () => {
    if (!file) return;
    setLoading(true);
    try {
      await uploadVideo(file, 7.5, 2);
      await fetchJobs();
      setFile(null);
    } catch (e) {
      alert('Upload failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="p-10 max-w-4xl mx-auto">
      <h1 className="text-3xl font-bold mb-8">FluxFrame Dashboard</h1>

      {/* Upload Section */}
      <div className="bg-slate-100 p-6 rounded-lg mb-10">
        <h2 className="text-xl font-semibold mb-4">New Job</h2>
        <input
          type="file"
          onChange={(e) => setFile(e.target.files?.[0] || null)}
          className="block w-full text-sm text-slate-500
            file:mr-4 file:py-2 file:px-4
            file:rounded-full file:border-0
            file:text-sm file:font-semibold
            file:bg-violet-50 file:text-violet-700
            hover:file:bg-violet-100
          "
        />
        <button
          onClick={handleUpload}
          disabled={!file || loading}
          className="mt-4 bg-blue-600 text-white px-4 py-2 rounded disabled:opacity-50"
        >
          {loading ? 'Uploading...' : 'Start Interpolation'}
        </button>
      </div>

      {/* Jobs List */}
      <div>
        <h2 className="text-xl font-semibold mb-4">Recent Jobs</h2>
        <div className="space-y-4">
          {jobs.map((job) => (
            <div key={job._id} className="border p-4 rounded flex justify-between items-center">
              <div>
                <div className="font-bold">{job._id}</div>
                <div className="text-sm text-gray-500">Status: {job.status}</div>
                <div className="text-xs text-gray-400">Video: {job.video_id}</div>
              </div>
              <Link href={`/jobs/${job._id}`} className="text-blue-500 hover:underline">
                View Details
              </Link>
            </div>
          ))}
        </div>
      </div>
    </main>
  );
}
