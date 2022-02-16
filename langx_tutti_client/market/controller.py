from typing import Optional
import hashlib

class TuttiMarketController:
    def __init__(self, duct):
        self._duct = duct

    async def open(self, wsd_url: str):
        await self._duct.open(wsd_url)

    async def register_job(self, job_class_id: str, job_parameter: Optional[dict] = True, description: Optional[str] = None, num_job_assignments_max: Optional[int] = None, priority_score: Optional[int] = None):
        data = await self._duct.call(self._duct.EVENT['REGISTER_JOB'], {
                'access_token': self.access_token,
                'job_class_id': job_class_id,
                'job_parameter': job_parameter,
                'description': description,
                'num_job_assignments_max': num_job_assignments_max,
                'priority_score': priority_score
            })
        return data

    async def sign_in(self, user_id: str, password: str, access_token_lifetime: int):
        data = await self._duct.call(self._duct.EVENT['SIGN_IN'], {
            'user_id': user_id,
            'password_hash': hashlib.sha512(password.encode('ascii')).digest(),
            'access_token_lifetime': access_token_lifetime
        })
        self.access_token = data['body']['access_token']

    async def sign_out(self):
        data = await self._duct.call(self._duct.EVENT['SIGN_OUT'], {
            'access_token': self.access_token
        })
