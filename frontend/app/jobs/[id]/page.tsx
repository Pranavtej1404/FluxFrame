'use client';

import { useEffect, useState, use } from 'react';
import { getJob } from '@/lib/api';

export default function JobDetails({ params }: { params: Promise<{ id: string }> }) {
    const [job, setJob] = useState<any>(null);

    // Unwrap params using React.use()
    const { id } = use(params);

    useEffect(() => {
        if (id) {
            loadJob(id);
            const interval = setInterval(() => loadJob(id), 2000);
            return () => clearInterval(interval);
        }
    }, [id]);

    const loadJob = async (jobId: string) => {
        try {
            const data = await getJob(jobId);
            setJob(data);
        } catch (e) {
            console.error(e);
        }
    };

    if (!job) return <div className="p-10">Loading...</div>;

    return (
        <main className="p-10 max-w-4xl mx-auto">
            <h1 className="text-3xl font-bold mb-4">Job Details</h1>
            <div className="bg-slate-50 p-6 rounded-lg border">
                <div className="mb-4">
                    <span className="font-bold">ID:</span> {job._id}
                </div>
                <div className="mb-4">
                    <span className="font-bold">Status:</span>
                    <span className={`ml-2 px-2 py-1 rounded text-sm ${job.status === 'completed' ? 'bg-green-100 text-green-800' : 'bg-yellow-100 text-yellow-800'
                        }`}>
                        {job.status}
                    </span>
                </div>

                <h3 className="font-bold mt-6 mb-2">History</h3>
                <ul className="text-sm space-y-2">
                    {job.history?.map((h: any, i: number) => (
                        <li key={i} className="text-gray-600">
                            [{new Date(h.timestamp).toLocaleTimeString()}] {h.status}
                        </li>
                    ))}
                </ul>
            </div>
        </main>
    );
}
